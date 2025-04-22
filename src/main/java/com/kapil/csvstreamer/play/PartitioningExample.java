package com.kapil.csvstreamer.play;

import java.util.*;
import java.util.stream.Collectors;

public class PartitioningExample {
    public static void main(String[] args) {
        List<Customer> customers = Arrays.asList(
                new Customer("Kapil", true),
                new Customer("Sidhu", false),
                new Customer("Simran", true),
                new Customer("Manpreet", false),
                new Customer("Abhi", true)
        );

        // ✅ Partition by premium status
        Map<Boolean, List<Customer>> partitioned = customers.stream()
                .collect(Collectors.partitioningBy(Customer::isPremium));

        List<Customer> premium = partitioned.get(true);
        List<Customer> regular = partitioned.get(false);

        System.out.println("💎 Premium Customers:");
        premium.forEach(c -> System.out.println(" - " + c.getName()));

        System.out.println("\n👥 Regular Customers:");
        regular.forEach(c -> System.out.println(" - " + c.getName()));
    }

    static class Customer {
        private final String name;
        private final boolean isPremium;

        public Customer(String name, boolean isPremium) {
            this.name = name;
            this.isPremium = isPremium;
        }

        public String getName() { return name; }
        public boolean isPremium() { return isPremium; }
    }
}
