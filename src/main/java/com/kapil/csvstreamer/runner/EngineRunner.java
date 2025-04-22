package com.kapil.csvstreamer.runner;

import com.kapil.csvstreamer.processor.CsvProcessor;
import com.kapil.csvstreamer.util.FileUtils;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class EngineRunner implements CommandLineRunner {

    private final List<CsvProcessor> processors;

    public EngineRunner(List<CsvProcessor> processors) {
        this.processors = processors;
    }

    @Override
    public void run(String... args) throws Exception {
        for (CsvProcessor processor : processors) {
            System.out.println("\n==> Running: " + processor.name());
            FileUtils.logMemoryUsage();
            processor.process();
            FileUtils.logMemoryUsage();
        }

        FileUtils.cleanupTempFiles();
    }
}
