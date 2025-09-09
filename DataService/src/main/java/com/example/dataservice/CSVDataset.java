/*
package com.example.dataservice;

import jakarta.persistence.Entity;

import java.util.Arrays;
import java.util.Objects;


public class CSVDataset {

    private char delim;
    private boolean header;

    public CSVDataset(String name, String description, byte[] content,  char delim, boolean header) {
        //super(name, description, FileFormat.CSV, content);
        this.delim = delim;
        this.header = header;
    }

    public CSVDataset() {}

    public char getDelim() {

        return this.delim;
    }

    public boolean isHeader() {

        return this.header;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o)
            return true;
        if (!(o instanceof Dataset))
            return false;
        if (!(o instanceof CSVDataset))
            return false;
        CSVDataset dataset = (CSVDataset) o;
        return Objects.equals(this.getId(), dataset.getId()) &&
                Objects.equals(this.getName(), dataset.getName()) &&
                Objects.equals(this.getDescription(), dataset.getDescription()) &&
                Objects.equals(this.getDelim(), dataset.getDelim()) &&
                Objects.equals(this.isHeader(), dataset.isHeader()) &&
                Arrays.equals(this.getContent(), dataset.getContent());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getId(), this.getName(), this.getDescription(), this.getDelim(), this.isHeader(), Arrays.hashCode(this.getContent()));
    }

    @Override
    public String toString() {
        return "Dataset{" + "id=" + this.getId() + ", name='" + this.getName() + '\'' + ", description='" + this.getDescription()
                + '\'' + ", format=CSV" + ", delimiter='"+ this.getDelim() +'\'' +", header= "+ this.isHeader()
                + ", numAttributes=" + this.getNumAttributes() + ", numEntries="
                + this.getNumEntries() + ", createdAt=" + this.getCreatedAt() + '}';
    }
}
*/
