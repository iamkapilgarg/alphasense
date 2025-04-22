package com.kapil.csvstreamer.processor;

import com.kapil.csvstreamer.service.S3Service;
import com.kapil.csvstreamer.service.ZipExtractor;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Order(3)
@Component
public class Question3Processor implements CsvProcessor {

    private final S3Service s3Service;
    private final ZipExtractor zipExtractor;

    public Question3Processor(S3Service s3Service, ZipExtractor zipExtractor) {
        this.s3Service = s3Service;
        this.zipExtractor = zipExtractor;
    }

    @Override
    public void process() {
        Optional<String> key = s3Service.findZipKeyContaining("ques3");
        if (key.isEmpty()) {
            System.err.println("❌ No ZIP found for question3");
            return;
        }

        File zipFile = s3Service.downloadZip(key.get());
        Map<String, File> extractedFiles = zipExtractor.extract(zipFile);

        File accountsCsv = extractedFiles.get("accounts.csv");
        File transactionsCsv = extractedFiles.get("transactions.csv");

        if (accountsCsv == null || transactionsCsv == null) {
            System.err.println("Required CSVs (accounts.csv or transactions.csv) not found for question3");
            return;
        }

        Map<String, String> accountIdToName = loadAccountIdToName(accountsCsv);

        try (Stream<String> lines = Files.lines(transactionsCsv.toPath(), StandardCharsets.UTF_8)) {
            lines
                    .skip(1) // skip header
                    .map(line -> line.split(",", -1))
                    .filter(parts -> parts.length >= 3)
                    .forEach(parts -> {
                        String txnId = parts[0].trim();
                        String accountId = parts[1].trim();
                        String amount = parts[2].trim();
                        String accountName = accountIdToName.getOrDefault(accountId, "UNKNOWN");

                        System.out.printf("Transaction %s by %s: $%s%n", txnId, accountName, amount);
                    });

        } catch (IOException e) {
            System.err.println("❌ Error reading transactions.csv");
            e.printStackTrace();
        }
    }

    private Map<String, String> loadAccountIdToName(File accountsCsv) {
        try (Stream<String> lines = Files.lines(accountsCsv.toPath(), StandardCharsets.UTF_8)) {
            return lines
                    .skip(1) // skip header
                    .map(line -> line.split(",", -1))
                    .filter(parts -> parts.length >= 2)
                    .collect(Collectors.toMap(
                            parts -> parts[0].trim(),
                            parts -> parts[1].trim()
                    ));
        } catch (IOException e) {
            System.err.println("❌ Error reading accounts.csv");
            e.printStackTrace();
            return Map.of(); // empty map fallback
        }
    }

    @Override
    public String name() {
        return "question3";
    }
}