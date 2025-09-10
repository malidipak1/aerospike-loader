package com.aerospike.load.bitmap;

import com.aerospike.client.*;
import com.aerospike.client.operation.HLLOperation;
import com.aerospike.client.operation.HLLPolicy;
import com.aerospike.client.operation.HLLWriteFlags;
import com.aerospike.client.policy.ClientPolicy;
import com.google.common.hash.Hashing;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class HLLIngestion {
    private static Logger log = LogManager.getLogger(SegmentIngestion.class);
    private static final int BATCH_SIZE = 10000;
    private static final String BIN_NAME = "users_IN";
    private static final int INDEX_BITS = 15;
    //indexBits = 14 is good enough (< 1% error).
    //indexBits = 15 → 32,768 registers → ~0.58% error
    //indexBits = 16 → 65,536 registers → ~0.41% error
    private static final int MINHASH_BITS = 0;
    /**
     * Used for Jaccard similarity between HLLs (e.g., estimating overlap).
     * If you only need counts and Boolean queries (AND, OR, NOT), set minhashBits = 0.
     * If you also want to compute audience similarity (e.g., overlap %), then set minhashBits = 4–8 (trade-off: more memory).
     */


    private AerospikeClient client;
    private String namespace;
    private String setName;
    private ExecutorService executor;
    private final ConcurrentHashMap<String, Object> segmentLocks = new ConcurrentHashMap<>();

    private static final int scaleFactor = 5;

    private HLLIngestion(AerospikeClient client,
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

        final HLLIngestion hllIngestion = new HLLIngestion(aerospikeClient, namespace, setName, writerThread);
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
        final ConcurrentHashMap<String, Set<ByteArrayWrapper>> perSegmentMap = new ConcurrentHashMap<>();
        for (final String line : batch) {
            executor.submit(() -> processLine(line, perSegmentMap, latch));
        }
        latch.await();

        saveToAerospike(perSegmentMap);
        log.info("Batch of {} lines finished in {} ms.", batchSize, (System.currentTimeMillis() - startTime));
    }

    private void saveToAerospike(ConcurrentHashMap<String, Set<ByteArrayWrapper>> perSegmentMap) throws InterruptedException{
        final CountDownLatch latch = new CountDownLatch(perSegmentMap.size());
        for (Map.Entry<String, Set<ByteArrayWrapper>> e : perSegmentMap.entrySet()) {
            final String seg = e.getKey();
            final Set<ByteArrayWrapper> users = e.getValue();
            if (users.isEmpty()) continue;

            List<byte[]> values = users.stream().map(ByteArrayWrapper::get).collect(Collectors.toList());
            executor.submit(() -> addBatchToSegmentWithRetries(seg, values, latch));
        }
        latch.await();
    }

    private void addBatchToSegmentWithRetries(String segmentId, List<byte[]> hashedValues, final CountDownLatch latch) {
        try {
            final List<Value> users = new ArrayList<>();
            for(final byte[] bytes : hashedValues) {
                users.add(Value.get(bytes));
            }

            final HLLPolicy hllPolicy = new HLLPolicy(HLLWriteFlags.DEFAULT | HLLWriteFlags.NO_FAIL);
            final Operation addOp = HLLOperation.add(hllPolicy, BIN_NAME, users, INDEX_BITS, MINHASH_BITS);
            final Key key = new Key(namespace, setName, segmentId);
            int maxRetries = 3;
            int attempt = 0;
            long sleep = 50l;
            while (attempt <= maxRetries) {
                try {
                    client.operate(null, key, addOp);
                    return;
                } catch (Exception ex) {
                    attempt++;
                    if (attempt > maxRetries) {
                        log.error("Failed to add to segment %s after %d attempts: %s%n", segmentId, attempt - 1, ex.getMessage());
                        return;
                    }
                    try {
                        Thread.sleep(sleep);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                    sleep = Math.min(sleep * 2, 100L);
                }
            }
        } finally {
            latch.countDown();
        }
    }

    private void processLine(final String line, final ConcurrentHashMap<String, Set<ByteArrayWrapper>> perSegmentMap, CountDownLatch latch) {
        try {
            String[] parts = line.split("\\|");
            if (parts.length < 2) return;

            final String userId = parts[0];
            final ByteArrayWrapper userHash = hashUserId(userId);
            String[] segments = parts[1].split(",");

            for (final String seg : segments) {
                if (!seg.isEmpty()) {
                    final Object segmentLock = segmentLocks.computeIfAbsent(seg, k -> new Object());
                    synchronized (segmentLock) {
                        perSegmentMap
                                .computeIfAbsent(seg, k -> new HashSet<>())
                                .add(userHash);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error while processing line ", e);
        } finally {
            latch.countDown();
        }
    }



    /** Hash a string -> 64-bit and return 8-byte big-endian array for Aerospike HLL add */
    private static ByteArrayWrapper hashUserId(String userId) {
        long h = Hashing.murmur3_128().hashString(userId, StandardCharsets.UTF_8).asLong();
        return new ByteArrayWrapper(longToBytesBE(h));
    }

    private static byte[] longToBytesBE(long v) {
        return new byte[] {
                (byte)(v >>> 56),
                (byte)(v >>> 48),
                (byte)(v >>> 40),
                (byte)(v >>> 32),
                (byte)(v >>> 24),
                (byte)(v >>> 16),
                (byte)(v >>> 8),
                (byte)(v)
        };
    }


}
