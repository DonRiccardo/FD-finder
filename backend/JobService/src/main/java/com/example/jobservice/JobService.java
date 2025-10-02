package com.example.jobservice;

import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.stereotype.Service;

import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Service
public class JobService {

    private final String jobResultsDirectory = "jobs/results/";

    public void uploadFile(Long jobId, MultipartFile file) {

        final String filename = getResultsFileName(jobId);

        saveFileData(file, getResultsFilePath(filename));
    }

    private Path getResultsFilePath(String fileName) {

        return Paths.get(this.jobResultsDirectory, fileName).toAbsolutePath().normalize();
    }

    public String getResultsFileName(Long jobId) {

        return "job-" + jobId + "-foundFDs.txt";
    }

    private void saveFileData(MultipartFile file, Path path) {
        try {
            Files.createDirectories(path.getParent());
            file.transferTo(path.toFile());
        } catch (Exception e) {
            throw new RuntimeException("Failed to save dataset file", e);
        }
    }

    public Resource getFile(Long jobId) throws IOException {
        Path path = getResultsFilePath(getResultsFileName(jobId));

        if(!Files.exists(path)) {
            throw new ResultsNotFoundException(jobId);
        }

        return new UrlResource(path.toUri());

    }
}
