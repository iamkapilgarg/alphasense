package com.kapil.csvstreamer.processor;

import com.kapil.csvstreamer.service.S3Service;
import com.kapil.csvstreamer.service.ZipExtractor;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Order(1)
@Component
public class Question1Processor implements CsvProcessor {

    private final S3Service s3Service;
    private final ZipExtractor zipExtractor;

    public Question1Processor(S3Service s3Service, ZipExtractor zipExtractor) {
        this.s3Service = s3Service;
        this.zipExtractor = zipExtractor;
    }

    @Override
    public void process() {
        List<String> zipKeys = s3Service.listAllZipKeysInFolder("company-data/");
        for (String key : zipKeys) {
            if (key.equals("company-data/MNZIRS0108.zip")) {
                question1();
            }
            if (key.equals("company-data/Y1HZ7B0146.zip")) {
                question2();
            }
            if (key.equals("company-data/U07N2S0124.zip")) {
                question3();
            }
        }
        question4();
    }

    public void question3() {
        File zipFile = s3Service.downloadCSV("company-data/U07N2S0124.zip");
        Map<String, File> extractedFiles = zipExtractor.extract(zipFile);

        File csvFile = extractedFiles.get("u07n2s0124.csv");
        if (csvFile == null) {
            System.err.println("csv not found for question1");
            return;
        }

        try (Stream<String> lines = Files.lines(csvFile.toPath(), StandardCharsets.UTF_8)) {
            List<String> allLines = lines.toList();

            String[] header = allLines.get(0).split(",");
            int targetColumnIndex = -1;

            for (int i = 0; i < header.length; i++) {
                if ("2015-09-30".equals(header[i].trim())) {
                    targetColumnIndex = i;
                    break;
                }
            }
            if (targetColumnIndex == -1) {
                System.out.println("Question 3: Column not found: 2015-09-30");
                return;
            }

            for (int i = 1; i < allLines.size(); i++) {
                String[] parts = allLines.get(i).split(",");
                System.out.println(parts.toString());
                if ("MO_BS_Intangibles".equals(parts[0].trim())) {
                    System.out.println("Question 3: MO_BS_Intangibles 2015-09-30 Value " + parts[targetColumnIndex]);
                    break;
                }
            }

        } catch (IOException e) {
            System.err.println("Failed to process CSV file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void question2() {
        File zipFile = s3Service.downloadCSV("company-data/Y1HZ7B0146.zip");
        Map<String, File> extractedFiles = zipExtractor.extract(zipFile);

        File csvFile = extractedFiles.get("y1hz7b0146.csv");
        if (csvFile == null) {
            System.err.println("csv not found for question1");
            return;
        }

        try (Stream<String> lines = Files.lines(csvFile.toPath(), StandardCharsets.UTF_8)) {
            OptionalDouble mean = lines
                    .skip(1) // skip header
                    .map(line -> line.split(","))
                    .filter(parts -> parts.length > 2 && "MO_BS_AP".equals(parts[0].trim()))
                    .flatMapToDouble(parts -> {
                        return java.util.stream.IntStream.range(2, parts.length)
                                .mapToDouble(i -> {
                                    try {
                                        return Double.parseDouble(parts[i].trim());
                                    } catch (NumberFormatException e) {
                                        return Double.NaN;
                                    }
                                });
                    })
                    .filter(d -> !Double.isNaN(d))
                    .average();

            if (mean.isPresent()) {
                System.out.printf("Question 2: Mean of row MO_BS_AP: %.2f%n", mean.getAsDouble());
            } else {
                System.out.println("Could not calculate mean (row not found or no numeric values)");
            }

        } catch (IOException e) {
            System.err.println("Failed to process CSV file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void question1() {
        {
            File zipFile = s3Service.downloadCSV("company-data/MNZIRS0108.zip");
            Map<String, File> extractedFiles = zipExtractor.extract(zipFile);

            File csvFile = extractedFiles.get("mnzirs0108.csv");
            if (csvFile == null) {
                System.err.println("transactions.csv not found for question1");
                return;
            }

            try (Stream<String> lines = Files.lines(csvFile.toPath(), StandardCharsets.UTF_8)) {
                List<String> allLines = lines.toList();

                String[] header = allLines.get(0).split(",");
                int targetColumnIndex = -1;

                for (int i = 0; i < header.length; i++) {
                    if ("2014-10-01".equals(header[i].trim())) {
                        targetColumnIndex = i;
                        break;
                    }
                }

                for (int i = 1; i < allLines.size(); i++) {
                    String[] parts = allLines.get(i).split(",");
                    if ("MO_BS_INV".equals(parts[0].trim())) {
                        System.out.println("Question 1: MO_BS_INV 2014-10-01 Value " + parts[targetColumnIndex]);
                        break;
                    }
                }

            } catch (IOException e) {
                System.err.println("Failed to process CSV file: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    public void question4() {
        double globalSum = 0;
        int globalCount = 0;

        List<String> zipKeys = s3Service.listAllZipKeysInFolder("company-data");

        for (String key : zipKeys) {
            File zipFile = s3Service.downloadCSV(key);
            Map<String, File> extractedFiles = zipExtractor.extract(zipFile);

            for (Map.Entry<String, File> entry : extractedFiles.entrySet()) {
                File csvFile = entry.getValue();

                try (Stream<String> lines = Files.lines(csvFile.toPath(), StandardCharsets.UTF_8)) {
                    Optional<double[]> result = lines
                            .skip(1)
                            .map(line -> line.split(","))
                            .filter(parts -> parts.length > 2 && "MO_BS_AP".equals(parts[0].trim()))
                            .map(parts -> {
                                double sum = 0;
                                int count = 0;
                                for (int i = 2; i < parts.length; i++) {
                                    try {
                                        sum += Double.parseDouble(parts[i].trim());
                                        count++;
                                    } catch (NumberFormatException ignored) {}
                                }
                                return new double[]{sum, count};
                            })
                            .findFirst();

                    if (result.isPresent()) {
                        globalSum += result.get()[0];
                        globalCount += result.get()[1];
                    }

                } catch (IOException e) {
                    System.err.printf("Failed to process %s: %s%n", entry.getKey(), e.getMessage());
                    e.printStackTrace();
                }
            }
        }

        if (globalCount > 0) {
            double overallMean = globalSum / globalCount;
            System.out.printf("Question 4: Overall mean of MO_BS_AP: %.2f%n", overallMean);
        } else {
            System.out.println("MO_BS_AP row not found or no valid numeric data in any file.");
        }
    }

    @Override
    public String name() {
        return "question 1";
    }


}
