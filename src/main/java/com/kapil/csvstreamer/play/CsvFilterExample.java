package com.kapil.csvstreamer.play;

import java.util.*;
import java.util.stream.Collectors;

public class CsvFilterExample {

    public static void main(String[] args) {
        List<Transaction> transactions = Arrays.asList(
                new Transaction("user1", "Book", "12.5", "North"),
                new Transaction(" ", "Pen", "5.0", "East"),
                new Transaction(null, "Notebook", "7.2", "South"),
                new Transaction("user4", "Pencil", "", "West"),
                new Transaction("user5", "Eraser", "abc", "North"),
                new Transaction("user6", "Scale", null, "East")
        );

        List<Transaction> cleanTransactions = transactions.stream()
                .filter(t -> t.getUserId() != null && !t.getUserId().trim().isEmpty())
                .filter(t -> t.getAmount() != null && !t.getAmount().trim().isEmpty())
                .filter(t -> isNumeric(t.getAmount()))
                .collect(Collectors.toList());

        System.out.println("✅ Clean Transactions:");
        cleanTransactions.forEach(t ->
                System.out.println(t.getUserId() + " → " + t.getAmount() + " (" + t.getProduct() + ")")
        );
    }

    private static boolean isNumeric(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    static class Transaction {
        private String userId;
        private String product;
        private String amount;
        private String region;

        public Transaction(String userId, String product, String amount, String region) {
            this.userId = userId;
            this.product = product;
            this.amount = amount;
            this.region = region;
        }

        public String getUserId() {
            return userId;
        }

        public String getProduct() {
            return product;
        }

        public String getAmount() {
            return amount;
        }

        public String getRegion() {
            return region;
        }
    }
}
