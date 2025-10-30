package cz.cuni.mff.fdfinder.dataservice;

public class DatasetNotFoundException extends RuntimeException {
    public DatasetNotFoundException(Long id) {

        super("Could not found dataset with ID: " + id);
    }
}
