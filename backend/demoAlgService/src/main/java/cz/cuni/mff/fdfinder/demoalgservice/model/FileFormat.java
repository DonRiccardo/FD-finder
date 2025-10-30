package cz.cuni.mff.fdfinder.demoalgservice.model;

/**
 * Enum representing supported file formats.
 */
public enum FileFormat {
    CSV ("text/csv"),
    JSON ("text/json");

    public final String contentType;
    FileFormat(String contentType) {
        this.contentType = contentType;
    }

    public String getContentType() {
        return contentType;
    }
}
