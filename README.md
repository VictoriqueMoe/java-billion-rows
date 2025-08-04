# Billion Row Challenge - Java Implementation

A high-performance Java implementation of the [Billion Row Challenge](https://github.com/gunnarmorling/1brc) that processes one billion temperature measurements as fast as possible.

## Overview

This implementation processes a text file containing one billion rows of weather station temperature data in the format:
```
Station Name;Temperature
Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
```

## Features

- **Memory-mapped file I/O** using NIO FileChannel for maximum performance
- **Multi-threaded processing** utilizing all available CPU cores
- **Custom ByteArrayKey** for efficient string handling without repeated allocations
- **Optimized temperature parsing** with pre-computed lookup tables for generation
- **Data generator** to create test files with realistic weather station data
- **Chunked processing** with proper line boundary alignment
- **Zero external dependencies** - uses only Java standard library

## Requirements

- **Java 21** or higher

## Usage

### Compile the Project

```bash
# 1. First, make sure the output directory exists and compile
mkdir -p out
javac -d out src/main/java/moe/victorique/*.java

# 2. Copy the resources to the output directory
mkdir -p out/moe/victorique
copy src\main\resources\weather_stations.csv out\weather_stations.csv

# 3. Generate test data (use proper Windows classpath separator)
java -cp "out;src/main/resources" moe.victorique.BillionRowChallenge -generate

# 4. Run the challenge
java -cp "out;src/main/resources" moe.victorique.BillionRowChallenge
```

### With Maven

```bash
# Compile the project
mvn compile

# Generate data (1 billion rows)
mvn exec:java -Dexec.mainClass="moe.victorique.BillionRowChallenge" -Dexec.args="-generate"

# Process the data
mvn exec:java -Dexec.mainClass="moe.victorique.BillionRowChallenge"
```

The program will output results in the format:
```
{Abha=-23.0/18.0/59.2, Abidjan=-16.2/26.0/67.3, Abéché=-10.0/29.4/69.0, ...}

RESULTS
Total Time: PT5.136S
Processing Speed: 194.69 MB/s
```

## Performance Optimizations

- **Memory-mapped I/O**: Uses FileChannel.map() for zero-copy file access
- **Parallel processing**: Work distributed across all CPU cores using ExecutorService
- **ByteArrayKey optimization**: Avoids string creation during processing phase
- **Custom parsing**: Hand-optimized temperature parsing without regex or string splitting
- **Chunk-aligned processing**: File split at line boundaries for proper parallel processing
- **High load factor HashMap**: Uses 0.99 load factor to minimize memory overhead
- **Pre-computed temperature lookup**: Generation uses lookup table for faster data creation

## Architecture

### Core Components

- `BillionRowChallenge.java` - Main entry point and result coordination
- `BillionRowParser.java` - Core processing logic with memory-mapped parsing
- `DataGenerator.java` - Optimized test data generation with producer-consumer pattern
- `StationStats.java` - Temperature statistics accumulator

### Processing Flow

1. Memory-map the input file using FileChannel for direct access
2. Split file into chunks based on CPU core count
3. Align chunk boundaries on line breaks to ensure complete records
4. Process chunks in parallel using thread pool
5. Parse station names and temperatures using custom byte-level parsing
6. Accumulate statistics (min/max/sum/count) per station using ByteArrayKey
7. Merge results from all worker threads
8. Convert to string keys and sort alphabetically for output

## Building and Running


## Performance Notes

Typical performance on modern hardware:
- **Generation**: 50-150 million rows/second
- **Processing**: 200-400 million rows/second
- **I/O throughput**: 3-6 GB/second

Performance scales with:
- Number of CPU cores
- Available RAM for memory mapping
- Storage I/O speed (SSD vs HDD)
- JVM heap size and garbage collection settings

### JVM Tuning

For optimal performance, consider these JVM flags:
```bash
java -XX:+UseG1GC -Xmx8g -XX:+UnlockExperimentalVMOptions -XX:+UseZGC -cp "out:src/main/resources" moe.victorique.BillionRowChallenge
```
