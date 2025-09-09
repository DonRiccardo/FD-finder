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
public abstract class Dataset {

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

    private byte[] content;

    private LocalDateTime createdAt;

    public Dataset(String name, String description, FileFormat fileFormat, byte[] content) {
        this.name = name;
        this.description = description;
        this.fileFormat = fileFormat;
        this.content = content;
    }

    public Dataset() {

    }

    @PrePersist
    public void prePersist() {

        this.createdAt = LocalDateTime.now();
    }

    public Long getId() {

        return this.id;
    }

    public String getName() {

        return this.name;
    }

    public String getDescription() {

        return this.description;
    }

    public FileFormat getFileFormat() {

        return this.fileFormat;
    }

    public byte[] getContent() {

        return this.content;
    }

    public Long getNumEntries() {

        return this.numEntries;
    }

    public int getNumAttributes() {

        return this.numAttributtes;
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
                Arrays.equals(this.content, dataset.content);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id, this.name, this.description, this.fileFormat, Arrays.hashCode(this.content));
    }

    @Override
    public String toString() {
        return "Dataset{" + "id=" + this.id + ", name='" + this.name + '\'' + ", description='" + this.description
                + '\'' + ", format=" + this.fileFormat + ", numAttributes=" + this.numAttributtes + ", numEntries="
                + this.numEntries + ", createdAt=" + this.createdAt + '}';
    }
}
