package com.kapil.csvstreamer.model;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {
    private String transactionId;
    private String accountId;
    private String amount;
    private String timestamp;
}