package com.kapil.csvstreamer.play;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class NestedOperationsDemo {
    public static void main(String[] args) {
        List<Review> reviews = Arrays.asList(
                new Review("product1", "user1", 4),
                new Review("product1", "user2", 5),
                new Review("product1", "user3", 3),
                new Review("product2", "user2", 2),
                new Review("product2", "user1", 5),
                new Review("product3", "user3", 1),
                new Review("product3", "user2", 4)
        );

        Map<String, List<Integer>> ratingsByProduct = reviews.stream()
                .collect(Collectors.groupingBy(
                        Review::getProduct,
                        Collectors.mapping(Review::getRating, Collectors.toList())
                ));

        Map<String, Double> avgRatingByProduct = reviews.stream()
                .collect(Collectors.groupingBy(
                        Review::getProduct,
                        Collectors.averagingInt(Review::getRating)
                ));

        Map<String, Map<String, Integer>> productUserRatings = reviews.stream()
                .collect(Collectors.groupingBy(
                        Review::getProduct,
                        Collectors.toMap(Review::getUser, Review::getRating)
                ));

        Map<String, List<String>> topReviewers = reviews.stream()
                .filter(r -> r.getRating() >= 4)
                .collect(Collectors.groupingBy(
                        Review::getProduct,
                        Collectors.mapping(Review::getUser, Collectors.toList())
                ));

        // Print everything
        System.out.println("⭐ Ratings by Product:");
        ratingsByProduct.forEach((k, v) -> System.out.println(" - " + k + " → " + v));

        System.out.println("\n📊 Average Rating by Product:");
        avgRatingByProduct.forEach((k, v) -> System.out.printf(" - %s → %.2f%n", k, v));

        System.out.println("\n👥 Product → User Ratings:");
        productUserRatings.forEach((k, v) -> System.out.println(" - " + k + " → " + v));

        System.out.println("\n💎 Top Reviewers (4+):");
        topReviewers.forEach((k, v) -> System.out.println(" - " + k + " → " + v));
    }

    static class Review {
        String product;
        String user;
        int rating;

        public Review(String product, String user, int rating) {
            this.product = product;
            this.user = user;
            this.rating = rating;
        }

        public String getProduct() { return product; }
        public String getUser()    { return user; }
        public int getRating()     { return rating; }
    }
}
