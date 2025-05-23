package com.kapil.csvstreamer.service;

import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;

@Service
public class S3Service {

    private static final String BUCKET_NAME = "as-findata-tech-challenge"; // Replace dynamically if needed

    private final S3Client s3Client;

    public S3Service() {
        this.s3Client = S3Client.builder()
                .region(Region.US_WEST_2) // Adjust as needed
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .build();
    }

    public File downloadCSV(String csvKey) {
        File tempZip = null;
        try {
            tempZip = Files.createTempFile("s3-", ".csv").toFile();
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(csvKey)
                    .build();

            try (ResponseInputStream<GetObjectResponse> s3Stream = s3Client.getObject(request);
                 FileOutputStream fos = new FileOutputStream(tempZip)) {
                s3Stream.transferTo(fos);
            }

            //System.out.println("Downloaded CSV from S3: " + tempZip.getAbsolutePath());

        } catch (IOException e) {
            System.err.println("Failed to download CSV from S3: " + csvKey);
            e.printStackTrace();
        }

        return tempZip;
    }

    public Optional<String> findZipKeyContaining(String keyword) {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(BUCKET_NAME)
                .build();

        ListObjectsV2Response response = s3Client.listObjectsV2(request);

        return response.contents().stream()
                .map(S3Object::key)
                .filter(key -> key.toLowerCase().contains(keyword.toLowerCase()) && key.endsWith(".zip"))
                .findFirst();
    }

    public List<String> listAllZipKeysInFolder(String folderPrefix) {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(BUCKET_NAME)
                .prefix(folderPrefix)
                .build();

        ListObjectsV2Response response = s3Client.listObjectsV2(request);

        return response.contents().stream()
                .map(S3Object::key)
                .filter(key -> key.endsWith(".zip"))
                .toList();
    }
}
