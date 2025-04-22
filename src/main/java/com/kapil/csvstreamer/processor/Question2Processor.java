package com.kapil.csvstreamer.processor;

import com.kapil.csvstreamer.service.S3Service;
import com.kapil.csvstreamer.service.ZipExtractor;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Map;
import java.util.stream.Stream;

@Order(2)
@Component
public class Question2Processor implements CsvProcessor {

    private final S3Service s3Service;
    private final ZipExtractor zipExtractor;

    public Question2Processor(S3Service s3Service, ZipExtractor zipExtractor) {
        this.s3Service = s3Service;
        this.zipExtractor = zipExtractor;
    }

    @Override
    public void process() {
        File zipFile = s3Service.downloadZip("names.zip");
        Map<String, File> extractedFiles = zipExtractor.extract(zipFile);

        File input = extractedFiles.get("names.csv");
        if (input == null) {
            System.err.println("names.csv not found for question2");
            return;
        }

        Path outputPath = Path.of("src/main/resources/output/output-q2.csv");
        try {
            Files.createDirectories(outputPath.getParent());

            try (Stream<String> lines = Files.lines(input.toPath(), StandardCharsets.UTF_8);
                 BufferedWriter writer = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8, StandardOpenOption.CREATE)) {

                writer.write("fullName,lastName\n");

                lines
                        .skip(1) // Skip header
                        .map(line -> line.split(",", -1)) // Preserve empty strings
                        .filter(parts -> parts.length >= 3)
                        .map(parts -> {
                            String fullName = (parts[0].trim() + " " + parts[1].trim()).trim();
                            String lastName = parts[2].trim();
                            return fullName + "," + lastName;
                        })
                        .forEach(line -> {
                            try {
                                writer.write(line);
                                writer.newLine();
                            } catch (IOException e) {
                                System.err.println("[Q2] ❌ Failed to write line: " + line);
                            }
                        });

                System.out.println("[Q2] ✅ Output written to: " + outputPath.toAbsolutePath());

            }

        } catch (IOException e) {
            System.err.println("[Q2] ❌ Error during processing:");
            e.printStackTrace();
        }
    }

    @Override
    public String name() {
        return "question2";
    }
}