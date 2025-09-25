package com.example.dataservice;

public class UnsupportedFileFormatException extends RuntimeException {
    public UnsupportedFileFormatException(String format) {
        super("This file format is not supported: " + format);
    }
}
