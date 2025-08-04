package moe.victorique;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataGenerator {

    private static final ByteBuffer POISON_PILL = ByteBuffer.allocate(0);
    private static final int BUFFER_SIZE = 4 * 1024 * 1024; // 4 MB for fewer, larger writes
    private static final byte[][] NUMBER_LOOKUP;

    static {
        NUMBER_LOOKUP = new byte[1999][];
        for (int i = 0; i < 1999; i++) {
            double temp = (i - 999) / 10.0;
            NUMBER_LOOKUP[i] = String.format("%.1f", temp).getBytes(StandardCharsets.UTF_8);
        }
    }

    public void generate(Path stationsPath, Path output, long totalRows) throws IOException {
        Instant start = Instant.now();

        List<byte[]> stationBytes = loadStationsAsBytes(stationsPath);
        System.out.printf("Loaded and pre-converted %d weather stations.%n", stationBytes.size());
        System.out.printf("Generating %,d rows to %s...%n", totalRows, output.toAbsolutePath());

        int numWorkers = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);
        BlockingQueue<ByteBuffer> queue;
        Future<Void> writerFuture;
        List<Future<?>> workerFutures;
        try (ExecutorService executor = Executors.newFixedThreadPool(numWorkers)) {
            queue = new ArrayBlockingQueue<>(numWorkers * 2);

            writerFuture = startWriterThread(output, queue);

            workerFutures = new ArrayList<>();
            long rowsPerWorker = totalRows / numWorkers;
            for (int i = 0; i < numWorkers; i++) {
                final long startRow = i * rowsPerWorker;
                final long endRow = (i == numWorkers - 1) ? totalRows : startRow + rowsPerWorker;

                Future<?> future = executor.submit(() -> {
                    try {
                        generateDataChunk(stationBytes, startRow, endRow, queue);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                workerFutures.add(future);
            }

            executor.shutdown();
        }
        try {
            for (Future<?> future : workerFutures) {
                future.get();
            }
            queue.put(POISON_PILL);
            writerFuture.get();
        } catch (Exception e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        }

        Duration duration = Duration.between(start, Instant.now());
        System.out.printf("Generation complete in %s%n", duration);
        long fileSize = Files.size(output);
        long fileSizeGB = fileSize / (1024 * 1024 * 1024);
        System.out.printf("Created file: %s (%,d GB, %,d bytes)%n", output.toAbsolutePath(), fileSizeGB, fileSize);
    }

    private void generateDataChunk(List<byte[]> stationBytes, long startRow, long endRow, BlockingQueue<ByteBuffer> queue) throws InterruptedException {
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        Random random = ThreadLocalRandom.current();
        int stationCount = stationBytes.size();

        for (long i = startRow; i < endRow; i++) {
            byte[] currentStationBytes = stationBytes.get(random.nextInt(stationCount));
            // **OPTIMIZATION:** Generate an integer and use it to index the lookup table.
            byte[] tempBytes = NUMBER_LOOKUP[random.nextInt(1999)];

            if (buffer.remaining() < currentStationBytes.length + tempBytes.length + 2) {
                buffer.flip();
                queue.put(buffer);
                buffer = ByteBuffer.allocate(BUFFER_SIZE);
            }

            buffer.put(currentStationBytes);
            buffer.put((byte) ';');
            buffer.put(tempBytes);
            buffer.put((byte) '\n');
        }
        if (buffer.position() > 0) {
            buffer.flip();
            queue.put(buffer);
        }
    }

    private Future<Void> startWriterThread(Path outputPath, BlockingQueue<ByteBuffer> queue) {
        return Executors.newSingleThreadExecutor().submit(() -> {
            try (FileChannel channel = FileChannel.open(outputPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                while (true) {
                    ByteBuffer buffer = queue.take();
                    if (buffer == POISON_PILL) {
                        break;
                    }
                    channel.write(buffer);
                }
            } catch (IOException | InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            }
            return null;
        });
    }

    private List<byte[]> loadStationsAsBytes(Path stationsPath) throws IOException {
        try (Stream<String> lines = Files.lines(stationsPath, StandardCharsets.UTF_8)) {
            return lines
                    .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                    .map(line -> line.split(";")[0].getBytes(StandardCharsets.UTF_8))
                    .collect(Collectors.toList());
        }
    }
}
