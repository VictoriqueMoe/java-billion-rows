package moe.victorique;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class BillionRowChallenge {

    private static final Path DATA_FILE_PATH = Paths.get("data.txt");
    private static final Path STATIONS_FILE;

    static {
        try {
            STATIONS_FILE = Paths.get(Objects.requireNonNull(BillionRowChallenge.class.getResource("/weather_stations.csv")).toURI());
        } catch (URISyntaxException | NullPointerException e) {
            System.err.println("Could not find weather_stations.csv in resources. Falling back to current directory.");
            throw new RuntimeException("Failed to initialize file paths.", e);
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println("Billion row challenge Java version");

        if (args.length > 0 && args[0].equals("-generate")) {
            long rowsToGenerate = (args.length > 1) ? Long.parseLong(args[1]) : 1_000_000_000L;
            new DataGenerator().generate(STATIONS_FILE, DATA_FILE_PATH, rowsToGenerate);
            return;
        }

        if (!Files.exists(DATA_FILE_PATH)) {
            System.out.printf("Data file not found: %s. Please generate it first with the -generate flag.%n", DATA_FILE_PATH.toAbsolutePath());
            return;
        }

        Instant start = Instant.now();

        BillionRowParser parser = new BillionRowParser();
        Map<String, StationStats> finalStats = parser.process(DATA_FILE_PATH);

        Duration duration = Duration.between(start, Instant.now());
        printResults(new TreeMap<>(finalStats), duration, Files.size(DATA_FILE_PATH));
    }

    private static void printResults(Map<String, StationStats> stats, Duration duration, long fileSize) {
        String result = stats.entrySet().stream()
                .map(entry -> String.format("%s=%.1f/%.1f/%.1f",
                        entry.getKey(),
                        entry.getValue().getMin(),
                        entry.getValue().getAverage(),
                        entry.getValue().getMax()))
                .collect(Collectors.joining(", ", "{", "}"));

        System.out.println(result);
        System.out.printf("%nRESULTS%n");
        System.out.printf("Total Time: %s%n", duration);
        if (fileSize > 0 && !duration.isZero()) {
            double speed = fileSize / (1024.0 * 1024.0) / duration.toNanos() * 1_000_000_000.0;
            System.out.printf("Processing Speed: %.2f MB/s%n", speed);
        }
    }
}
