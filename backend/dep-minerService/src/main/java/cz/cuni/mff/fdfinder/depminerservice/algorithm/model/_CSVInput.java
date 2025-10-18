/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package cz.cuni.mff.fdfinder.depminerservice.algorithm.model;

import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 *
 * @author Richard
 */
public class _CSVInput extends _Input {

    public _CSVInput(String filePath, String tableName, boolean hasHeader, String delim, int skip, int limit, SparkSession spark) throws IOException {
        super(filePath, tableName, hasHeader, delim, skip, limit, spark);
    }

    @Override
    protected void readDataSpark(SparkSession spark) throws IOException {
        this.df = spark.read().option("header", this.hasHeader).option("delimiter", this.delimiter).csv(this.filePath.toString());
    }

}
