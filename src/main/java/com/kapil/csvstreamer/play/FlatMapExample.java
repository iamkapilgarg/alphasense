package com.kapil.csvstreamer.play;

import java.util.*;
import java.util.stream.Collectors;

public class FlatMapExample {
    public static void main(String[] args) {
        List<Order> orders = Arrays.asList(
                new Order("O1", Arrays.asList("apple", "banana", "grape")),
                new Order("O2", Arrays.asList("banana", "orange")),
                new Order("O3", Arrays.asList("apple", "kiwi"))
        );

        // 🧾 1. Extract all unique items across all orders
        Set<String> allUniqueItems = orders.stream()
                .flatMap(order -> order.getItems().stream()) // flattens List<String> into one stream
                .collect(Collectors.toSet());

        // 🧾 2. Count how many times each item was ordered
        Map<String, Long> itemCounts = orders.stream()
                .flatMap(order -> order.getItems().stream())
                .collect(Collectors.groupingBy(
                        item -> item,
                        Collectors.counting()
                ));

        // Output
        System.out.println("🛒 All Unique Items:");
        allUniqueItems.forEach(System.out::println);

        System.out.println("\n📦 Item Frequencies:");
        itemCounts.forEach((item, count) ->
                System.out.printf(" - %s → %d%n", item, count));
    }

    static class Order {
        String orderId;
        List<String> items;

        public Order(String orderId, List<String> items) {
            this.orderId = orderId;
            this.items = items;
        }

        public String getOrderId() { return orderId; }
        public List<String> getItems() { return items; }
    }
}
