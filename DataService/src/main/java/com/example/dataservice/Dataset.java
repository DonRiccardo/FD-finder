package com.example.dataservice;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
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
    private String name;
    private String description;
    @NotNull
    private FileFormat fileFormat;

    private int numAttributtes=0;
    private Long numEntries=0L;

    private String fileURL;
    private LocalDateTime createdAt;

    private String delim;
    private boolean header;

    public Dataset(String name, String description, String fileFormat, String fileURL, String delim, boolean header) {
        this.name = name;
        this.description = description;
        setFileFormat(fileFormat);
        this.fileURL = fileURL;
        this.delim = delim;
        this.header = header;
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

    public String getFileURL() {

        return this.fileURL;
    }

    public void setFileURL(String fileURL) {

        this.fileURL = fileURL;
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
                Objects.equals(this.fileURL, dataset.fileURL);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id, this.name, this.description, this.fileFormat, this.delim, this.header, this.fileURL);
    }

    @Override
    public String toString() {
        return "Dataset{" + "id=" + this.id + ", name='" + this.name + '\'' + ", fileURL=" + this.fileURL +
                ", description='" + this.description
                + '\'' + ", format=" + this.fileFormat + ", delimiter='"+ this.delim +'\'' +", header= "+ this.header
                + ", numAttributes=" + this.numAttributtes + ", numEntries="
                + this.numEntries + ", createdAt=" + this.createdAt + '}';
    }
}
