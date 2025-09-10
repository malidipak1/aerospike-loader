package com.aerospike.load.bitmap;

import com.aerospike.client.*;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.google.common.hash.Hashing;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SegmentIngestion {
    private static Logger log = LogManager.getLogger(SegmentIngestion.class);
    private static final int BATCH_SIZE = 10000;

    private final AerospikeClient client;
    private String namespace;
    private String setName;
    private final ExecutorService executor;
    private final ConcurrentHashMap<String, Roaring64NavigableMap> segmentBitmaps = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> segmentLocks = new ConcurrentHashMap<>();

    private static final int scaleFactor = 1;

    private SegmentIngestion(AerospikeClient client,
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

        final SegmentIngestion segmentIngestion = new SegmentIngestion(aerospikeClient, namespace, setName, writerThread);
        segmentIngestion.ingestFile(Paths.get(file));
    }

    public void ingestFile(final Path file) throws Exception {
        log.info("Processing file: " + file);
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

            saveToAerospike();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            executor.shutdown();
        }
        log.info("Finished processing the file: " + file);
    }

    private void processBatch(List<String> batch) throws InterruptedException {
        int batchSize = batch.size();
        final CountDownLatch latch = new CountDownLatch(batchSize);

        for (final String line : batch) {
            executor.submit(() -> processLine(line, latch));
        }

        latch.await();
        log.info("Main thread: Batch of {} lines finished.", batchSize);
    }

    private void processLine(final String line, CountDownLatch latch) {
        try {
            String[] parts = line.split("\\|");
            if (parts.length < 2) return;

            final String userId = parts[0];
            long userHash = hashUserId(userId);
            String[] segments = parts[1].split(",");

            for (final String seg : segments) {
                final Object segmentLock = segmentLocks.computeIfAbsent(seg, k -> new Object());
                synchronized (segmentLock) {
                    final Roaring64NavigableMap bitmap =
                            segmentBitmaps
                            .computeIfAbsent(seg, k -> new Roaring64NavigableMap());
                    bitmap.add(userHash);
                }
            }
        } catch (Exception e) {
            log.error("Error while processing line ", e);
        } finally {
            latch.countDown();
        }
    }

    private Roaring64NavigableMap loadSegment(final String segmentId) {
        Key key = new Key(namespace, setName, segmentId);
        Record rec = client.get(null, key);
        if (rec == null) return null;

        byte[] data = (byte[]) rec.getValue("users");
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
            Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
            bitmap.deserialize(dis);
            return bitmap;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void saveToAerospike() throws IOException {
        final WritePolicy policy = new WritePolicy();
        policy.recordExistsAction = RecordExistsAction.REPLACE;
        for (Map.Entry<String, Roaring64NavigableMap> entry : segmentBitmaps.entrySet()) {
            final String segmentId = entry.getKey();
            final Roaring64NavigableMap newBitmap = entry.getValue();

            // Step 1: Load existing bitmap from Aerospike
            Roaring64NavigableMap merged = loadSegment(segmentId);
            if (merged == null) {
                merged = new Roaring64NavigableMap();
            }

            // Step 2: Merge (bitwise OR)
            merged.or(newBitmap);

            // Step 3: Save back
            byte[] serialized = serializeBitmap(merged);
            Key key = new Key(namespace, setName, segmentId);
            Bin bin = new Bin("users", Value.get(serialized));
            client.put(policy, key, bin);

            log.info("segment: {}, total unique users: {}" ,segmentId, merged.getLongCardinality());
        }

        // Clear in-memory cache after flush
        segmentBitmaps.clear();
    }

    private byte[] serializeBitmap(Roaring64NavigableMap bitmap) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            DataOutputStream dos = new DataOutputStream(baos);
            bitmap.serialize(dos);
            return baos.toByteArray();
        }
    }

    private long hashUserId(String uuid) {
        return Hashing.murmur3_128().hashString(uuid, StandardCharsets.UTF_8).asLong();
        //But if you stick to .asLong(), you’re truncating to 64 bits, which is still fine —
        // collision risk is low until you get close to ~2^32 (~4B users). for more than 4B we should move to below
        //return Hashing.murmur3_128().hashString(uuid, StandardCharsets.UTF_8);
    }

}
