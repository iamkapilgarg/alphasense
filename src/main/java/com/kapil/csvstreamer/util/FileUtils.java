package com.kapil.csvstreamer.util;

import java.io.File;
import java.util.Objects;

public class FileUtils {

    public static void logMemoryUsage() {
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        System.out.printf("[Memory] Used: %.2f MB\n", usedMemory / (1024.0 * 1024.0));
    }

    public static void cleanupTempFiles() {
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        File[] tempFiles = tempDir.listFiles((dir, name) ->
                name.startsWith("s3-") || name.startsWith("unzipped-") || name.endsWith(".zip") || name.endsWith(".csv")
        );

        if (tempFiles != null) {
            for (File file : tempFiles) {
                if (file.delete()) {
                    System.out.println("Deleted temp file: " + file.getName());
                }
            }
        }
    }
}
