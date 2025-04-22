package com.kapil.csvstreamer.play;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

public class SortingPractice {

    public static void main(String[] args) {
        List<Transaction> transactions = Arrays.asList(
                new Transaction("T1", "A1", 12.5, LocalDate.of(2023, 12, 1)),
                new Transaction("T2", "A2", 50.0, LocalDate.of(2023, 11, 25)),
                new Transaction("T3", "A3", 9.5,  LocalDate.of(2023, 12, 5)),
                new Transaction("T4", "A4", 99.9, LocalDate.of(2023, 11, 20)),
                new Transaction("T5", "A5", 15.0, LocalDate.of(2023, 12, 3))
        );

        // 1. Sort by amount ASCENDING
        List<Transaction> sortedByAmountAsc = transactions.stream()
                .sorted(Comparator.comparing(Transaction::getAmount))
                .collect(Collectors.toList());

        // 2. Sort by amount DESCENDING
        List<Transaction> sortedByAmountDesc = transactions.stream()
                .sorted(Comparator.comparing(Transaction::getAmount).reversed())
                .collect(Collectors.toList());

        // 3. Sort by date ASC
        List<Transaction> sortedByDate = transactions.stream()
                .sorted(Comparator.comparing(Transaction::getDate))
                .collect(Collectors.toList());

        // 4. Sort by amount, then by date (compound sort)
        List<Transaction> sortedByAmountThenDate = transactions.stream()
                .sorted(Comparator.comparing(Transaction::getAmount)
                        .thenComparing(Transaction::getDate))
                .collect(Collectors.toList());

        // === OUTPUT ===
        System.out.println("💸 Sorted by Amount (ASC):");
        sortedByAmountAsc.forEach(System.out::println);

        System.out.println("\n💰 Sorted by Amount (DESC):");
        sortedByAmountDesc.forEach(System.out::println);

        System.out.println("\n📅 Sorted by Date:");
        sortedByDate.forEach(System.out::println);

        System.out.println("\n🔗 Sorted by Amount, then Date:");
        sortedByAmountThenDate.forEach(System.out::println);
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

        public String getTxnId() { return txnId; }
        public String getAccountId() { return accountId; }
        public double getAmount() { return amount; }
        public LocalDate getDate() { return date; }

        @Override
        public String toString() {
            return String.format("Txn{id=%s, account=%s, amount=%.2f, date=%s}", txnId, accountId, amount, date);
        }
    }
}
