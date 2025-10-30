package cz.cuni.mff.fdfinder.depminerservice.model;

import java.util.Objects;

/**
 * Class representing Dataset and its metadate.
 */
public class DatasetDto {

    private Long id;
    private String name;
    private FileFormat fileFormat;

    private String delim;
    private boolean header;

    public DatasetDto(Long id, String name, FileFormat fileFormat,
                      String delim, boolean header) {
        this.id = id;
        this.name = name;
        this.fileFormat = fileFormat;
        this.delim = delim;
        this.header = header;
    }

    public DatasetDto() {

    }

    /**
     *
     * @param fileFormat {@link FileFormat} of the file to be set
     */
    public void setFileFormat(FileFormat fileFormat) {

        this.fileFormat = fileFormat;
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
     * @return {@link FileFormat} format of the dataset
     */
    public FileFormat getFileFormat() {

        return this.fileFormat;
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

    @Override
    public boolean equals(Object o) {

        if (this == o)
            return true;
        if (!(o instanceof DatasetDto))
            return false;
        DatasetDto dataset = (DatasetDto) o;
        return Objects.equals(this.id, dataset.id) &&
                Objects.equals(this.name, dataset.name) &&
                Objects.equals(this.fileFormat, dataset.fileFormat) &&
                Objects.equals(this.delim, dataset.delim) &&
                Objects.equals(this.header, dataset.header);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id, this.name, this.fileFormat, this.delim, this.header);
    }

    /**
     *
     * @return {@link String} form of the {@link DatasetDto} metadata
     */
    @Override
    public String toString() {
        return "Dataset{" + "id=" + this.id + ", name='" + this.name + '\'' + ", fileName="
                + '\'' + ", format=" + this.fileFormat + ", delimiter='"+ this.delim +'\'' +", header= "+ this.header
                ;
    }


}
