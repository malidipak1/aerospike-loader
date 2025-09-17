package com.aerospike.load.bitmap;

import com.aerospike.client.*;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.WritePolicy;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class SketchIngestion {
    private static Logger log = LogManager.getLogger(SegmentIngestion.class);
    private static final int BATCH_SIZE = 75000;
    private static final String BIN_NAME = "users_sketch_IN";

    private AerospikeClient client;
    private String namespace;
    private String setName;
    private ExecutorService executor;
    private final ConcurrentHashMap<String, Object> segmentLocks = new ConcurrentHashMap<>();

    private static final int scaleFactor = 5;

    private SketchIngestion(AerospikeClient client,
                            final String namespace,
                            final String setName,
                            final int writerThread) {
        this.client = client;
        this.namespace = namespace;
        this.setName = setName;
        this.executor = Executors.newFixedThreadPool(writerThread);
    }

    public static void main(String[] argv) throws Exception {
        if (argv.length < 1) {
            log.error("Usage: ingest|query [options]");
            System.exit(2);
        }

        final Map<String, String> opts = new HashMap<>();
        final List<String> paths = new ArrayList<>();
        for (int i = 0; i < argv.length; i++) {
            String a = argv[i];
            if (a.startsWith("--")) {
                String key = a.substring(2);
                String val = "";
                if (i + 1 < argv.length && !argv[i + 1].startsWith("--")) {
                    val = argv[++i];
                }
                opts.put(key, val);
            } else {
                paths.add(a);
            }
        }

        String aeroHost = opts.getOrDefault("aeroHost", "127.0.0.1");
        int aeroPort = Integer.parseInt(opts.getOrDefault("aeroPort", "3000"));
        String namespace = opts.getOrDefault("ns", "test");
        String setName = opts.getOrDefault("set", "segments");
        String file = opts.get("file");

        int cpus = Runtime.getRuntime().availableProcessors();
        final int writerThread = (cpus * scaleFactor);

        final Host[] hosts = Host.parseHosts(aeroHost, aeroPort);
        final ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.setFailIfNotConnected(false);
        clientPolicy.maxConnsPerNode = writerThread + 1;

        final AerospikeClient aerospikeClient = new AerospikeClient(clientPolicy, hosts);

        final SketchIngestion hllIngestion = new SketchIngestion(aerospikeClient, namespace, setName, writerThread);
        hllIngestion.ingestFile(Paths.get(file));
    }

    public void ingestFile(final Path file) throws Exception {
        log.info("Processing file: " + file);
        long startTime = System.currentTimeMillis();

        try (BufferedReader reader = Files.newBufferedReader(file)) {
            String line;
            List<String> batch = new ArrayList<>(BATCH_SIZE);
            while ((line = reader.readLine()) != null) {
                batch.add(line);
                if (batch.size() == BATCH_SIZE) {
                    processBatch(batch);
                    batch = new ArrayList<>(BATCH_SIZE); // Prepare for next batch
                }
            }
            if (!batch.isEmpty()) {
                processBatch(batch);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.MINUTES);
        }
        log.info("Finished processing the file: {} took {} ms", file, (System.currentTimeMillis() - startTime));
    }

    private void processBatch(List<String> batch) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        int batchSize = batch.size();
        final CountDownLatch latch = new CountDownLatch(batchSize);
        final ConcurrentHashMap<String, UpdateSketch> perSegmentSketchesMap = new ConcurrentHashMap<>();

        for (final String line : batch) {
            executor.submit(() -> processLine(line, perSegmentSketchesMap, latch));
        }
        latch.await();

        saveToAerospike(perSegmentSketchesMap);
        log.info("Batch of {} lines finished in {} ms.", batchSize, (System.currentTimeMillis() - startTime));
    }

    private void saveToAerospike(final ConcurrentHashMap<String, UpdateSketch> perSegmentSketchesMap) throws InterruptedException{
        final CountDownLatch latch = new CountDownLatch(perSegmentSketchesMap.size());
        for (Map.Entry<String, UpdateSketch> entry : perSegmentSketchesMap.entrySet()) {
            final String seg = entry.getKey();
            final UpdateSketch userSketch = entry.getValue();
            if (userSketch.isEmpty()) continue;

            //List<byte[]> values = users.stream().map(ByteArrayWrapper::get).collect(Collectors.toList());
            executor.submit(() -> addBatchToSegmentWithRetries(seg, userSketch, latch));
        }
        latch.await();
    }

    private void addBatchToSegmentWithRetries(String segmentId, final UpdateSketch userSketch, final CountDownLatch latch) {
        try {
            int maxRetries = 3, retryCount = 0;
            final Key key = new Key(namespace, setName, segmentId);
            do {
                try {
                    final Record record = client.get(null, key);
                    Sketch existingSketch = null;
                    int generation = 0;

                    if (record != null) {
                        byte[] existingBytes = record.getBytes(BIN_NAME);
                        // Create a Union object to correctly merge sketches
                        final Union union = SetOperation.builder().buildUnion();
                        union.union(CompactSketch.heapify(Memory.wrap(existingBytes)));
                        union.union(userSketch);
                        existingSketch = union.getResult();
                        generation = record.generation;
                    } else {
                        existingSketch = userSketch.compact();
                    }

                    byte[] mergedBytes = existingSketch.compact().toByteArray();

                    final WritePolicy writePolicy = new WritePolicy();
                    //writePolicy.recordExistsAction = RecordExistsAction.REPLACE;
                    writePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
                    writePolicy.generation = generation;
                    client.put(writePolicy, key, new Bin(BIN_NAME, mergedBytes));
                    log.info("Successfully merged sketch for segment {} with count of {}", segmentId, existingSketch.getEstimate());
                    break; //break if all aerospike ops succeeded
                } catch (Exception e) {
                    if(retryCount >= maxRetries) {
                        log.error("Unable to connect aerospike for segment {}", segmentId);
                    }
                } finally {
                    retryCount++;
                }
            } while (retryCount < maxRetries);
        } finally {
            latch.countDown();
        }
    }

    private void processLine(final String line, final ConcurrentHashMap<String, UpdateSketch> perSegmentSketchesMap, CountDownLatch latch) {
        try {
            String[] parts = line.split("\\|");
            if (parts.length < 2) return;

            final String userId = parts[0];
            //final ByteArrayWrapper userHash = hashUserId(userId);
            String[] segments = parts[1].split(",");

            for (final String seg : segments) {
                if (!seg.isEmpty()) {
                    final Object segmentLock = segmentLocks.computeIfAbsent(seg, k -> new Object());
                    synchronized (segmentLock) {
                        final UpdateSketch sketch = perSegmentSketchesMap.computeIfAbsent(seg, s -> Sketches.updateSketchBuilder().build());
                        sketch.update(userId);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error while processing line ", e);
        } finally {
            latch.countDown();
        }
    }
}
