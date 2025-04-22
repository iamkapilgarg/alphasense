package com.kapil.csvstreamer.play;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;

public class MalformedCsvExample {

    public static void main(String[] args) {
        List<String[]> rawData = Arrays.asList(
                new String[]{"T1", "A1", "12.5", "2023-12-01"},
                new String[]{"T2", "A2", "abc", "2023-12-02"},      // invalid amount
                new String[]{"T3", "", "20.0", "2023-12-03"},        // blank accountId
                new String[]{"T4", "A4"},                            // too short
                new String[]{"T5", "A5", "30.5", "bad-date"},        // invalid date
                new String[]{"T6", "A6", "25.0", "2023-12-06"}       // valid
        );

        List<Transaction> validTransactions = rawData.stream()
                .map(MalformedCsvExample::parseSafe)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        System.out.println("✅ Valid Transactions:");
        validTransactions.forEach(System.out::println);
    }

    private static Optional<Transaction> parseSafe(String[] parts) {
        if (parts.length < 4) {
            System.err.println("❌ Skipping: not enough fields → " + Arrays.toString(parts));
            return Optional.empty();
        }

        String txnId = parts[0].trim();
        String accountId = parts[1].trim();
        String amountStr = parts[2].trim();
        String dateStr = parts[3].trim();

        if (txnId.isEmpty() || accountId.isEmpty() || amountStr.isEmpty() || dateStr.isEmpty()) {
            System.err.println("❌ Skipping: blank required field → " + Arrays.toString(parts));
            return Optional.empty();
        }

        double amount;
        try {
            amount = Double.parseDouble(amountStr);
        } catch (NumberFormatException e) {
            System.err.println("❌ Skipping: invalid amount → " + Arrays.toString(parts));
            return Optional.empty();
        }

        LocalDate date;
        try {
            date = LocalDate.parse(dateStr);
        } catch (DateTimeParseException e) {
            System.err.println("❌ Skipping: invalid date → " + Arrays.toString(parts));
            return Optional.empty();
        }

        return Optional.of(new Transaction(txnId, accountId, amount, date));
    }

    static class Transaction {
        private final String txnId;
        private final String accountId;
        private final double amount;
        private final LocalDate date;

        public Transaction(String txnId, String accountId, double amount, LocalDate date) {
            this.txnId = txnId;
            this.accountId = accountId;
            this.amount = amount;
            this.date = date;
        }

        @Override
        public String toString() {
            return String.format("Txn{id='%s', account='%s', amount=%.2f, date=%s}",
                    txnId, accountId, amount, date);
        }
    }
}
