package com.kapil.csvstreamer.play;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MappingPractice {

    public static void main(String[] args) {
        List<String[]> rawData = Arrays.asList(
                new String[]{"T1", "A1", "12.5", "2023-12-01", "North"},
                new String[]{"T2", "A2", "abc", "2023-12-02", "East"},
                new String[]{"T3", "A3", "", "2023-12-03", "South"},
                new String[]{"T4", "A4", "99.9", "bad-date", "West"},
                new String[]{"T5", "A5", "15.0", "2023-12-05", "North"}
        );

        List<Transaction> transactions = rawData.stream()
                .map(MappingPractice::safeMapToTransaction)
                .filter(t -> t != null) // Skip bad rows
                .collect(Collectors.toList());

        System.out.println("✅ Valid Transactions:");
        transactions.forEach(System.out::println);
    }

    private static Transaction safeMapToTransaction(String[] parts) {
        if (parts.length < 5) return null;

        String txnId = parts[0].trim();
        String accountId = parts[1].trim();
        String amountStr = parts[2].trim();
        String dateStr = parts[3].trim();
        String region = parts[4].trim();

        try {
            double amount = Double.parseDouble(amountStr);
            LocalDate date = LocalDate.parse(dateStr, DateTimeFormatter.ISO_LOCAL_DATE);
            return new Transaction(txnId, accountId, amount, date, region);
        } catch (NumberFormatException | DateTimeParseException e) {
            System.err.println("Skipping bad row: " + Arrays.toString(parts));
            return null;
        }
    }

    static class Transaction {
        private final String txnId;
        private final String accountId;
        private final double amount;
        private final LocalDate date;
        private final String region;

        public Transaction(String txnId, String accountId, double amount, LocalDate date, String region) {
            this.txnId = txnId;
            this.accountId = accountId;
            this.amount = amount;
            this.date = date;
            this.region = region;
        }

        @Override
        public String toString() {
            return String.format("Transaction{id='%s', account='%s', amount=%.2f, date=%s, region='%s'}",
                    txnId, accountId, amount, date, region);
        }
    }
}
