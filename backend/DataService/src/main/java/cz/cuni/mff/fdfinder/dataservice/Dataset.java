package cz.cuni.mff.fdfinder.dataservice;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Class representing Dataset and its metadate.
 */
@Entity
public class Dataset {

    @Id
    @GeneratedValue
    private Long id;
    @NotEmpty
    @Column(unique = true)
    private String name;
    private String description;
    @NotNull
    private FileFormat fileFormat;
    private Long size;

    private int numAttributtes=0;
    private Long numEntries=0L;

    private String originalFilename;
    private String hash;
    private LocalDateTime createdAt;
    private long savedAt;

    private String delim;
    private boolean header;

    public Dataset(String name, String description, String fileFormat, String originalFilename, String delim,
                   boolean header, String hash) {
        this.name = name;
        this.description = description;
        setFileFormat(fileFormat);
        this.originalFilename = originalFilename;
        this.delim = delim;
        this.header = header;
        this.hash = hash;
    }

    public Dataset() {

    }

    @PrePersist
    public void prePersist() {

        this.createdAt = LocalDateTime.now();
    }

    /**
     * Set {@code fileFormat} based on the provided param.
     * @param fileFormat {@link String} file format
     */
    public void setFileFormat(String fileFormat) {
        fileFormat = fileFormat.toLowerCase().trim();
        if(fileFormat.contains("csv")) {
            this.fileFormat = FileFormat.CSV;
        }
        else if(fileFormat.contains("json")) {
            this.fileFormat = FileFormat.JSON;
        }
        else {
            throw new UnsupportedFileFormatException(fileFormat);
        }
    }

    /**
     *
     * @return {@link Long} ID of the dataset
     */
    public Long getId() {

        return this.id;
    }

    /**
     *
     * @param id {@link Long} ID of the dataset
     */
    public void setId(Long id) {

        this.id = id;
    }

    /**
     *
     * @return {@link String} name of the dataset
     */
    public String getName() {

        return this.name;
    }

    /**
     *
     * @param name {@link String} name of the dataset
     */
    public void setName(String name) {

        this.name = name;
    }

    /**
     *
     * @return {@link String} description of the dataset
     */
    public String getDescription() {

        return this.description;
    }

    /**
     *
     * @param description {@link String} description of the dataset
     */
    public void setDescription(String description) {

        this.description = description;
    }

    /**
     *
     * @return {@link FileFormat} format of the dataset
     */
    public FileFormat getFileFormat() {

        return this.fileFormat;
    }

    /**
     *
     * @return {@link String} original name of the dataset file provided by the user
     */
    public String getOriginalFilename() {

        return this.originalFilename;
    }

    /**
     *
     * @param filename {@link String} original name of the dataset file provided by the user
     */
    public void setOriginalFilename(String filename) {

        this.originalFilename = filename;
    }

    /**
     *
     * @return {@link Long} number of entries (rows) in the dataset
     */
    public Long getNumEntries() {

        return this.numEntries;
    }

    /**
     *
     * @param numEntries {@link Long} number of entries (rows) in the dataset
     */
    public void setNumEntries(Long numEntries) {

        this.numEntries = numEntries;
    }

    /**
     *
     * @return {@link Integer} number of attributes in the dataset
     */
    public int getNumAttributes() {

        return this.numAttributtes;
    }

    /**
     *
     * @param numAttributes {@link Integer} number of attributes in the dataset
     */
    public void setNumAttributes(int numAttributes) {

        this.numAttributtes = numAttributes;
    }

    /**
     *  Useful only for datasets in CSV format.
     * @return {@link String} delimiter separating values in the dataset
     */
    public String getDelim() {

        return this.delim;
    }

    /**
     * Useful only for datasets in CSV format.
     * @param delim {@link String} delimiter separating values in the dataset
     */
    public void setDelim(String delim) {

        this.delim = delim;
    }

    /**
     *
     * @return {@link Boolean} if the dataset contains header
     */
    public boolean getHeader() {

        return this.header;
    }

    /**
     *
     * @param header {@link Boolean} if the dataset contains header
     */
    public void setHeader(boolean header) {

        this.header = header;
    }

    /**
     *
     * @return {@link String} hash of the file content
     */
    public String getHash() {

        return this.hash;
    }

    /**
     *
     * @param hash {@link String} hash of the file content
     */
    public void setHash(String hash) {

        this.hash = hash;
    }

    /**
     *
     * @return {@link Long} size of the file in Bytes
     */
    public Long getSize() {

        return this.size;
    }

    /**
     *
     * @param size {@link Long} size of the file in Bytes
     */
    public void setSize(Long size) {

        this.size = size;
    }

    /**
     *
     * @return {@link Long} when the dataset was created
     */
    public LocalDateTime getCreatedAt() {

        return this.createdAt;
    }

    /**
     *
     * @param createdAt {@link Long} when the dataset was created
     */
    public void setCreatedAt(LocalDateTime createdAt) {

        this.createdAt = createdAt;
    }

    /**
     *
     * @return {@link Long} when the dataset file was saved
     */
    public long getSavedAt() {

        return this.savedAt;
    }

    /**
     *
     * @param savedAt {@link Long} when the dataset file was saved
     */
    public void setSavedAt(long savedAt) {

        this.savedAt = savedAt;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o)
            return true;
        if (!(o instanceof Dataset))
            return false;
        Dataset dataset = (Dataset) o;
        return Objects.equals(this.id, dataset.id) &&
                Objects.equals(this.name, dataset.name) &&
                Objects.equals(this.description, dataset.description) &&
                Objects.equals(this.fileFormat, dataset.fileFormat) &&
                Objects.equals(this.delim, dataset.delim) &&
                Objects.equals(this.header, dataset.header) &&
                Objects.equals(this.originalFilename, dataset.originalFilename) &&
                Objects.equals(this.hash, dataset.hash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id, this.name, this.description, this.fileFormat, this.delim, this.header, this.originalFilename);
    }

    /**
     *
     * @return {@link String} form of the {@link Dataset} metadata
     */
    @Override
    public String toString() {
        return "Dataset{" + "id=" + this.id + ", name='" + this.name + '\'' + ", fileName=" + this.originalFilename +
                ", description='" + this.description
                + '\'' + ", format=" + this.fileFormat + ", delimiter='"+ this.delim +'\'' +", header= "+ this.header
                + ", numAttributes=" + this.numAttributtes + ", numEntries="
                + this.numEntries + ", createdAt=" + this.createdAt + '}';
    }
}
