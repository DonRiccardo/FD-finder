package cz.cuni.mff.fdfinder.dataservice;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class UnsupportedFileFormatAdvice {

    @ExceptionHandler(UnsupportedFileFormatException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public String unsupportedFileFormatException(UnsupportedFileFormatException ex) {

        return ex.getMessage();
    }
}
