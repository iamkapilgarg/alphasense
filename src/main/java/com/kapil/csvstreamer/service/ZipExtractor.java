package com.kapil.csvstreamer.service;

import org.apache.commons.io.IOUtils;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Service
public class ZipExtractor {

    public Map<String, File> extract(File zipFile) {
        Map<String, File> extractedFiles = new HashMap<>();
        try (ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFile))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (!entry.getName().endsWith(".csv")) continue;

                File tempCsv = Files.createTempFile("unzipped-", ".csv").toFile();
                try (FileOutputStream fos = new FileOutputStream(tempCsv)) {
                    IOUtils.copy(zis, fos);
                }

                extractedFiles.put(new File(entry.getName()).getName().toLowerCase(), tempCsv);
                zis.closeEntry();
            }
        } catch (IOException e) {
            System.err.println("Failed to extract zip file: " + zipFile.getName());
            e.printStackTrace();
        }
        return extractedFiles;
    }
}
