package com.example.jobservice;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ResultsNotFoundAdvice {

    @ExceptionHandler(ResultsNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public String handleException(ResultsNotFoundException ex) {
        return ex.getMessage();
    }
}
