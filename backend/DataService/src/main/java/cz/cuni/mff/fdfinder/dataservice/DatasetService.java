package cz.cuni.mff.fdfinder.dataservice;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.concurrent.CompletableFuture;

/**
 * Service doing preparation, processing and get work for Dataset.
 */
@Service
public class DatasetService {

    private final String datasetDirectory = "datasets/";

    public DatasetService() {

    }

    /**
     * Record to store data about Dataset.
     * @param hash {@link String} of the file content
     * @param originalName {@link String} original file name provided by user
     * @param savedAt {@link Long} when the file was saved
     */
    public record FileResponse(

            String hash,
            String originalName,
            long savedAt
    ) implements Serializable {};

    /**
     * Save the dataset file to a folder locally.
     * @param file {@link MultipartFile} to be saved
     * @param fileFormat {@link FileFormat} of the file
     * @return {@link FileResponse} of the saved file
     */
    public FileResponse uploadFile(MultipartFile file, FileFormat fileFormat) {

        final String filename = getFilename(file);
        final String hash = computeFileHash(file);
        long savedAt = System.currentTimeMillis();

        saveFileData(file, getFilePath(hash, fileFormat, savedAt));

        return new FileResponse(hash, filename, savedAt);
    }

    /**
     * Record to store numbers of the dataset.
     * @param numAttributes {@link Integer} number of attributes in the dataset
     * @param numEntries {@link Long} number of entries (rows) in the dataset
     */
    public record FileNumbers(

            int numAttributes,
            Long numEntries
    ) implements Serializable {};

    /**
     * Read the {@link Dataset} and count number of entries and attributes, based on {@link FileFormat}.
     * @param dataset {@link Dataset} ready to be counted
     * @return {@link CompletableFuture} of {@link FileNumbers} of the dataset
     */
    @Async
    public CompletableFuture<FileNumbers> processNewDatasetNumbers(Dataset dataset){
        if (dataset.getFileFormat() == FileFormat.CSV) {
            return CompletableFuture.completedFuture(processNewCSVDataset(dataset));

        } else if (dataset.getFileFormat() == FileFormat.JSON) {
            return CompletableFuture.completedFuture(processNewJsonDataset(dataset));
        }

        return CompletableFuture.completedFuture(new FileNumbers(0, 0L));
    }

    /**
     * Read CSV file and count number of attributes and entries (rows).
     * @param dataset {@link Dataset} to be processed
     * @return {@link FileNumbers} with counted numbers
     */
    private FileNumbers processNewCSVDataset(Dataset dataset){
        int numAttributes = 0;
        long numEntries = 0L;

        try{
            Path filePath = getFilePath(dataset.getHash(), dataset.getFileFormat(), dataset.getSavedAt());
            try(BufferedReader br = Files.newBufferedReader(filePath)){

                String line = br.readLine();
                numAttributes = line.split(dataset.getDelim()).length;
                numEntries = dataset.getHeader() ? 0L : 1L;

                while ((line = br.readLine()) != null){
                    numEntries++;
                }

            }
        }
        catch (Exception e){

        }

        return new FileNumbers(numAttributes, numEntries);
    }

    /**
     * Read JSON file and count number of attributes and entries.
     * @param dataset {@link Dataset} to be processed
     * @return {@link FileNumbers} with counted numbers
     */
    private FileNumbers processNewJsonDataset(Dataset dataset){
        int numAttributes = 0;
        long numEntries = 0L;

        try{
            Path filePath = getFilePath(dataset.getHash(), dataset.getFileFormat(), dataset.getSavedAt());
            try(BufferedReader br = Files.newBufferedReader(filePath)){

                JsonElement jsonElement = JsonParser.parseReader(br);
                JsonArray jsonArray = jsonElement.getAsJsonArray();

                numEntries = jsonArray.size();

                for (int i = 0; i < jsonArray.size(); i++) {
                    int att = jsonArray.get(i).getAsJsonObject().size();
                    if (numAttributes < att) numAttributes = att;

                }
            }
        }
        catch (Exception e){

        }

        return new FileNumbers(numAttributes, numEntries);
    }

    /**
     * Create {@link Resource} to the specified dataset to be downloaded.
     * @param dataset {@link Dataset} you want to download
     * @return {@link Resource} for the dataset file
     * @throws IOException if the URL is Malformed
     */
    public Resource getFile(Dataset dataset) throws IOException {
        Path path = getFilePath(dataset.getHash(), dataset.getFileFormat(), dataset.getSavedAt());

        if(!Files.exists(path)) {
            throw new DatasetNotFoundException(dataset.getId());
        }

        return new UrlResource(path.toUri());

    }

    /**
     * Get filepath where the dataset file is saved or will be saved.
     * @param hash {@link String} of the file content
     * @param fileFormat {@link FileFormat} of the file
     * @param savedAt {@link Long} when the file was saved
     * @return {@link Path} to file location
     */
    private Path getFilePath(String hash, FileFormat fileFormat, long savedAt) {

        return Paths.get(this.datasetDirectory, fileFormat.toString().toLowerCase(), savedAt + "_" + hash).toAbsolutePath().normalize();
    }

    /**
     * Compute hash of the file content
     * @param file {@link MultipartFile} file to be hashed
     * @return {@link String} computed hash
     */
    private String computeFileHash(MultipartFile file) {

        try {
            final byte[] hash = MessageDigest.getInstance("SHA-256").digest(file.getBytes());
            return bytesToHex(hash);
        } catch (Exception e) {
            throw new RuntimeException("Failed to compute file hash", e);
        }
    }

    /**
     * Array to convert bytes to HEX
     */
    private static final byte[] HEX_ARRAY = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);

    /**
     * Converts bytes to HEX {@link String}
     * @param bytes to be converted
     * @return {@link String} HEX of the {@code bytes}
     */
    private static String bytesToHex(byte[] bytes) {
        byte[] hexChars = new byte[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars, StandardCharsets.UTF_8);
    }

    /**
     * Get original file name provided by the user.
     * @param file {@link MultipartFile} to obtain name
     * @return {@link String} original file name
     */
    private String getFilename(MultipartFile file) {
        String name = "newDataset";
        String filename = file.getOriginalFilename();
        if (filename != null)
            name = Paths.get(filename).getFileName().toString();

        return name;
    }

    /**
     * Save file locally to a specified {@link Path}
     * @param file {@link MultipartFile} to be saved
     * @param path {@link Path} where to save the file
     */
    private void saveFileData(MultipartFile file, Path path) {
        try {
            Files.createDirectories(path.getParent());
            file.transferTo(path.toFile());
        } catch (Exception e) {
            throw new RuntimeException("Failed to save dataset file", e);
        }
    }

    /**
     * Delete dataset file. (NOT metadata)
     * @param dataset {@link Dataset} to be deleted
     */
    public void deleteDatasetFile(Dataset dataset) {
        try {
            Path datasetPath = getFilePath(dataset.getHash(), dataset.getFileFormat(), dataset.getSavedAt());
            Files.deleteIfExists(datasetPath);
        }
        catch (IOException e) {
            System.err.println("Failed to delete dataset file: " + dataset.getHash());
        }
    }

}
