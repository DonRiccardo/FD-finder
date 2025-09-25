package com.example.dataservice;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Objects;


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

    private String delim;
    private boolean header;

    public Dataset(String name, String description, String fileFormat, String originalFilename, String delim, boolean header, String hash) {
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

    public Long getId() {

        return this.id;
    }

    public void setId(Long id) {

        this.id = id;
    }

    public String getName() {

        return this.name;
    }

    public void setName(String name) {

        this.name = name;
    }

    public String getDescription() {

        return this.description;
    }

    public void setDescription(String description) {

        this.description = description;
    }

    public FileFormat getFileFormat() {

        return this.fileFormat;
    }

    public String getOriginalFilename() {

        return this.originalFilename;
    }

    public void setOriginalFilename(String filename) {

        this.originalFilename = filename;
    }

    public Long getNumEntries() {

        return this.numEntries;
    }

    public int getNumAttributes() {

        return this.numAttributtes;
    }

    public String getDelim() {

        return this.delim;
    }

    public void setDelim(String delim) {

        this.delim = delim;
    }

    public boolean getHeader() {

        return this.header;
    }

    public void setHeader(boolean header) {

        this.header = header;
    }

    public String getHash() {

        return this.hash;
    }

    public void setHash(String hash) {

        this.hash = hash;
    }

    public Long getSize() {

        return this.size;
    }

    public void setSize(Long size) {

        this.size = size;
    }

    public LocalDateTime getCreatedAt() {

        return this.createdAt;
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

    @Override
    public String toString() {
        return "Dataset{" + "id=" + this.id + ", name='" + this.name + '\'' + ", fileName=" + this.originalFilename +
                ", description='" + this.description
                + '\'' + ", format=" + this.fileFormat + ", delimiter='"+ this.delim +'\'' +", header= "+ this.header
                + ", numAttributes=" + this.numAttributtes + ", numEntries="
                + this.numEntries + ", createdAt=" + this.createdAt + '}';
    }
}
