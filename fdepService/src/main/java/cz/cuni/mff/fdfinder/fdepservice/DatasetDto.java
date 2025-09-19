package cz.cuni.mff.fdfinder.fdepservice;

import jakarta.persistence.PrePersist;

import java.io.File;
import java.time.LocalDateTime;

public class DatasetDto {

    private Long id;
    private String name;
    private String description;
    private FileFormat fileFormat;

    private String fileURL;

    private String delim;
    private boolean header;

    public DatasetDto(Long id, String name, String description, FileFormat fileFormat, String fileURL, String delim, boolean header) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.fileFormat = fileFormat;
        this.fileURL = fileURL;
        this.delim = delim;
        this.header = header;
    }

    public DatasetDto() {

    }

    public void setFileFormat(FileFormat fileFormat) {

        this.fileFormat = fileFormat;
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


}
