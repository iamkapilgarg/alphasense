package com.kapil.csvstreamer.play;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class PeekExample {

    public static void main(String[] args) {
        List<Transaction> transactions = Arrays.asList(
                new Transaction("T1", "A1", 45.0, LocalDate.of(2023, 12, 1)),
                new Transaction("T2", "A2", 100.0, LocalDate.of(2023, 12, 2)),
                new Transaction("T3", "A3", 75.5, LocalDate.of(2023, 12, 3)),
                new Transaction("T4", "A4", 30.0, LocalDate.of(2023, 12, 4))
        );

        List<String> highValueTxnIds = transactions.stream()
                .peek(t -> System.out.println("🔍 Original: " + t))
                .filter(t -> t.getAmount() > 50)
                .peek(t -> System.out.println("✅ Passed filter (>50): " + t))
                .map(Transaction::getTxnId)
                .peek(id -> System.out.println("📦 Mapping to txnId: " + id))
                .collect(Collectors.toList());

        System.out.println("\n🎯 Final High Value Txn IDs: " + highValueTxnIds);
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

        public String getTxnId()     { return txnId; }
        public String getAccountId() { return accountId; }
        public double getAmount()    { return amount; }
        public LocalDate getDate()   { return date; }

        @Override
        public String toString() {
            return String.format("Txn{id=%s, acc=%s, amt=%.2f, date=%s}", txnId, accountId, amount, date);
        }
    }
}
