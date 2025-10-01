package com.example.dataservice;

import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;

@Service
public class DatasetService {

    private final String datasetDirectory = "datasets/";

    public DatasetService() {

    }

    public record FileResponse(

            String hash,
            String originalName
    ) implements Serializable {};

    public FileResponse uploadFile(MultipartFile file, FileFormat fileFormat) {

        final String filename = getFilename(file);
        final String hash = computeFileHash(file);

        saveFileData(file, getFilePath(hash, fileFormat));

        return new FileResponse(hash, filename);
    }

    public record FileNumbers(

            int numAttributes,
            Long numEntries
    ) implements Serializable {};

    @Async
    public FileNumbers processNewDataset(Dataset dataset){
        int numAttributes = 0;
        Long numEntries = 0L;

        try{
            Path filePath = getFilePath(dataset.getHash(), dataset.getFileFormat());
            try(BufferedReader br = Files.newBufferedReader(filePath)){

                String line = br.readLine();
                numAttributes = line.split(dataset.getDelim()).length;
                numEntries = dataset.getHeader() ? 0L : 1L;

                while (line != null){
                    line = br.readLine();
                    numEntries++;
                }

            }
        }
        catch (Exception e){

        }

        return new FileNumbers(numAttributes, numEntries);
    }

    public Resource getFile(Dataset dataset) throws IOException {
        Path path = getFilePath(dataset.getHash(), dataset.getFileFormat());

        if(!Files.exists(path)) {
            throw new DatasetNotFoundException(dataset.getId());
        }

        return new UrlResource(path.toUri());

    }

    private Path getFilePath(String hash, FileFormat fileFormat) {

        return Paths.get(this.datasetDirectory, fileFormat.toString().toLowerCase(), hash).toAbsolutePath().normalize();
    }

    private String computeFileHash(MultipartFile file) {

        try {
            final byte[] hash = MessageDigest.getInstance("SHA-256").digest(file.getBytes());
            return bytesToHex(hash);
        } catch (Exception e) {
            throw new RuntimeException("Failed to compute file hash", e);
        }
    }

    private static final byte[] HEX_ARRAY = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);
    private static String bytesToHex(byte[] bytes) {
        byte[] hexChars = new byte[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars, StandardCharsets.UTF_8);
    }

    private String getFilename(MultipartFile file) {
        String name = "newDataset";
        String filename = file.getOriginalFilename();
        if (filename != null)
            name = Paths.get(filename).getFileName().toString();

        return name;
    }

    private void saveFileData(MultipartFile file, Path path) {
        try {
            Files.createDirectories(path.getParent());
            file.transferTo(path.toFile());
        } catch (Exception e) {
            throw new RuntimeException("Failed to save dataset file", e);
        }
    }

}
