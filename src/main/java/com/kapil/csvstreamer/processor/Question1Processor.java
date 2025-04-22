package com.kapil.csvstreamer.processor;

import com.kapil.csvstreamer.model.Transaction;
import com.kapil.csvstreamer.service.S3Service;
import com.kapil.csvstreamer.service.ZipExtractor;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import java.util.Objects;
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
        File zipFile = s3Service.downloadZip("transaction.zip");
        Map<String, File> extractedFiles = zipExtractor.extract(zipFile);

        File csvFile = extractedFiles.get("transaction.csv");
        if (csvFile == null) {
            System.err.println("transactions.csv not found for question1");
            return;
        }

        try (Stream<String> lines = Files.lines(csvFile.toPath(), StandardCharsets.UTF_8)) {
            Map<String, Double> accountTotals = lines
                    .skip(1) // Skip header
                    .map(this::parseTransaction)
                    .filter(Objects::nonNull)
                    .filter(t -> t.getAccountId() != null && !t.getAccountId().trim().isEmpty())
                    .filter(t -> isNumeric(t.getAmount()))
                    .collect(Collectors.groupingBy(
                            Transaction::getAccountId,
                            Collectors.summingDouble(t -> Double.parseDouble(t.getAmount()))
                    ));

            System.out.println("[Q1] ✅ Account Totals:");
            accountTotals.forEach((account, total) ->
                    System.out.printf("Account %s: %.2f%n", account, total));

        } catch (IOException e) {
            System.err.println("❌ Failed to process CSV file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private Transaction parseTransaction(String line) {
        try {
            String[] parts = line.split(",", -1);
            if (parts.length < 3) return null;

            String txnId = parts[0].trim();       // optional
            String accountId = parts[1].trim();   // used for grouping
            String amount = parts[2].trim();      // needs parsing
            String region = parts.length > 3 ? parts[3].trim() : null;

            return new Transaction(txnId, accountId, amount, region);
        } catch (Exception e) {
            System.err.println("Skipping malformed line: " + line);
            return null;
        }
    }

    private boolean isNumeric(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException ex) {
            return false;
        }
    }

    @Override
    public String name() {
        return "question1";
    }
}