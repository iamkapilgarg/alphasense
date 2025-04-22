package com.kapil.csvstreamer.play;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

public class AggregationPractice {

    public static void main(String[] args) {
        List<Transaction> transactions = Arrays.asList(
                new Transaction("T1", "A1", 12.5, LocalDate.of(2023, 12, 1), "North"),
                new Transaction("T2", "A1", 8.0,  LocalDate.of(2023, 12, 2), "North"),
                new Transaction("T3", "A2", 15.0, LocalDate.of(2023, 12, 3), "East"),
                new Transaction("T4", "A3", 9.5,  LocalDate.of(2023, 12, 4), "South"),
                new Transaction("T5", "A2", 4.0,  LocalDate.of(2023, 12, 5), "East")
        );

        // 1. Total amount
        double totalAmount = transactions.stream()
                .mapToDouble(Transaction::getAmount)
                .sum();

        // 2. Count of transactions
        long count = transactions.size();

        // 3. Average amount
        double average = transactions.stream()
                .mapToDouble(Transaction::getAmount)
                .average()
                .orElse(0.0);

        // 4. Min and Max
        double min = transactions.stream()
                .mapToDouble(Transaction::getAmount)
                .min()
                .orElse(0.0);

        double max = transactions.stream()
                .mapToDouble(Transaction::getAmount)
                .max()
                .orElse(0.0);

        // 5. Total amount per region
        Map<String, Double> totalByRegion = transactions.stream()
                .collect(Collectors.groupingBy(
                        Transaction::getRegion,
                        Collectors.summingDouble(Transaction::getAmount)
                ));

        // 6. Transaction count per account
        Map<String, Long> countByAccount = transactions.stream()
                .collect(Collectors.groupingBy(
                        Transaction::getAccountId,
                        Collectors.counting()
                ));

        // 7. Average per region
        Map<String, Double> averageByRegion = transactions.stream()
                .collect(Collectors.groupingBy(
                        Transaction::getRegion,
                        Collectors.averagingDouble(Transaction::getAmount)
                ));

        // === OUTPUT ===
        System.out.println("💰 Total Amount = " + totalAmount);
        System.out.println("🧮 Total Transactions = " + count);
        System.out.println("📊 Average Amount = " + average);
        System.out.println("📉 Min Amount = " + min + ", 📈 Max Amount = " + max);

        System.out.println("\n🌍 Total Amount by Region:");
        totalByRegion.forEach((region, total) ->
                System.out.printf("  %s → %.2f%n", region, total));

        System.out.println("\n👤 Transaction Count by Account:");
        countByAccount.forEach((account, cnt) ->
                System.out.printf("  %s → %d%n", account, cnt));

        System.out.println("\n📈 Average Amount by Region:");
        averageByRegion.forEach((region, avg) ->
                System.out.printf("  %s → %.2f%n", region, avg));
        System.out.println("hello");
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

        public String getTxnId()     { return txnId; }
        public String getAccountId() { return accountId; }
        public double getAmount()    { return amount; }
        public LocalDate getDate()   { return date; }
        public String getRegion()    { return region; }
    }
}
