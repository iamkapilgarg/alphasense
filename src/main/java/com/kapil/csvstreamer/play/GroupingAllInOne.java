package com.kapil.csvstreamer.play;

import java.util.*;
import java.util.stream.Collectors;

public class GroupingAllInOne {

    public static void main(String[] args) {
        List<Person> people = Arrays.asList(
                new Person("Kapil", "Canada"),
                new Person("Sidhu", "India"),
                new Person("Simran", "Canada"),
                new Person("Manpreet", "India"),
                new Person("John", "USA")
        );

        List<Review> reviews = Arrays.asList(
                new Review("product1", 4),
                new Review("product2", 3),
                new Review("product1", 5),
                new Review("product2", 5),
                new Review("product3", 1),
                new Review("product3", 3)
        );

        // 1. Group people into lists by country
        Map<String, List<Person>> groupedByCountry = people.stream()
                .collect(Collectors.groupingBy(Person::getCountry));

        // 2. Count how many people in each country
        Map<String, Long> countByCountry = people.stream()
                .collect(Collectors.groupingBy(Person::getCountry, Collectors.counting()));

        // 3. Sum of ratings by product
        Map<String, Integer> sumRatingsByProduct = reviews.stream()
                .collect(Collectors.groupingBy(
                        Review::getProduct,
                        Collectors.summingInt(Review::getRating)
                ));

        // 4. Group people by country but store only names
        Map<String, List<String>> namesByCountry = people.stream()
                .collect(Collectors.groupingBy(
                        Person::getCountry,
                        Collectors.mapping(Person::getName, Collectors.toList())
                ));

        // ===== OUTPUT =====

        System.out.println("👥 1. People grouped by country:");
        groupedByCountry.forEach((country, list) -> {
            System.out.println(" - " + country + ": " + list.stream().map(Person::getName).collect(Collectors.joining(", ")));
        });

        System.out.println("\n🧮 2. Count of people by country:");
        countByCountry.forEach((country, count) ->
                System.out.printf(" - %s → %d%n", country, count));

        System.out.println("\n💰 3. Sum of product ratings:");
        sumRatingsByProduct.forEach((product, sum) ->
                System.out.printf(" - %s → %d%n", product, sum));

        System.out.println("\n📋 4. Names grouped by country:");
        namesByCountry.forEach((country, names) ->
                System.out.printf(" - %s → %s%n", country, names));
    }

    // === Supporting Classes ===
    static class Person {
        private final String name;
        private final String country;

        public Person(String name, String country) {
            this.name = name;
            this.country = country;
        }

        public String getName() { return name; }
        public String getCountry() { return country; }
    }

    static class Review {
        private final String product;
        private final int rating;

        public Review(String product, int rating) {
            this.product = product;
            this.rating = rating;
        }

        public String getProduct() { return product; }
        public int getRating() { return rating; }
    }
}
