package moe.victorique;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class BillionRowParser {

    private static final class ByteArrayKey {
        private final byte[] bytes;
        private final int hash;

        public ByteArrayKey(byte[] bytes) {
            this.bytes = bytes;
            this.hash = Arrays.hashCode(bytes);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            ByteArrayKey that = (ByteArrayKey) obj;
            return Arrays.equals(bytes, that.bytes);
        }

        public String asString() {
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    public Map<String, StationStats> process(Path filePath) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(filePath.toFile(), "r");
             FileChannel fileChannel = file.getChannel()) {

            long fileSize = fileChannel.size();
            int numThreads = Runtime.getRuntime().availableProcessors();
            List<Future<Map<ByteArrayKey, StationStats>>> futures;
            try (ExecutorService executor = Executors.newFixedThreadPool(numThreads)) {
                futures = new ArrayList<>();

                long chunkSize = fileSize / numThreads;
                long start = 0;

                for (int i = 0; i < numThreads; i++) {
                    long end = (i == numThreads - 1) ? fileSize : start + chunkSize;
                    end = findNextNewline(fileChannel, end, fileSize);

                    final long finalStart = start;
                    final long finalEnd = end;
                    futures.add(executor.submit(() -> processChunk(fileChannel, finalStart, finalEnd)));
                    start = end;
                }

                executor.shutdown();
            }

            Map<String, StationStats> finalResults = new TreeMap<>();
            try {
                for (Future<Map<ByteArrayKey, StationStats>> future : futures) {
                    Map<ByteArrayKey, StationStats> chunkResult = future.get();
                    chunkResult.forEach((key, stats) ->
                            finalResults.merge(key.asString(), stats, StationStats::merge));
                }
            } catch (Exception e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            }

            return finalResults;
        }
    }

    private Map<ByteArrayKey, StationStats> processChunk(FileChannel channel, long start, long end) throws IOException {
        Map<ByteArrayKey, StationStats> results = new HashMap<>(16384, 0.99f);
        long size = end - start;
        if (size <= 0) {
            return results;
        }

        ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, start, size);
        byte[] stationNameHolder = new byte[100];

        while (buffer.hasRemaining()) {
            int lineStartPos = buffer.position();
            int semicolonPos = -1;

            for (int i = lineStartPos; i < buffer.limit(); i++) {
                if (buffer.get(i) == ';') {
                    semicolonPos = i;
                    break;
                }
            }
            if (semicolonPos == -1) {
                break;
            }

            int nameLen = semicolonPos - lineStartPos;
            buffer.get(lineStartPos, stationNameHolder, 0, nameLen);
            ByteArrayKey key = new ByteArrayKey(Arrays.copyOf(stationNameHolder, nameLen));

            long temperature = 0;
            boolean negative = false;
            int tempStartPos = semicolonPos + 1;

            if (buffer.get(tempStartPos) == '-') {
                negative = true;
                tempStartPos++;
            }

            int lineEndPos = -1;
            for (int i = tempStartPos; i < buffer.limit(); i++) {
                byte b = buffer.get(i);
                if (b == '.') {
                    continue;
                }
                if (b == '\n') {
                    lineEndPos = i;
                    break;
                }
                temperature = temperature * 10 + (b - '0');
            }
            if (lineEndPos == -1) {
                break;
            }

            if (negative) {
                temperature = -temperature;
            }

            StationStats stats = results.get(key);
            if (stats == null) {
                results.put(key, new StationStats(temperature));
            } else {
                stats.update(temperature);
            }

            buffer.position(lineEndPos + 1);
        }
        return results;
    }

    private long findNextNewline(FileChannel channel, long position, long fileSize) throws IOException {
        if (position >= fileSize) {
            return fileSize;
        }
        long remaining = fileSize - position;
        ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, position, Math.min(1024, remaining));
        while (buffer.hasRemaining()) {
            if (buffer.get() == '\n') {
                return position + buffer.position();
            }
        }
        return fileSize;
    }
}
