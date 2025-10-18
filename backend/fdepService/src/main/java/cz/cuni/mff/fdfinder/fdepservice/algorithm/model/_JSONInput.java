package cz.cuni.mff.fdfinder.fdepservice.algorithm.model;

import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class _JSONInput extends _Input{

    public _JSONInput(String filePath, String tableName, int skip, int limit, SparkSession spark) throws IOException {
        super(filePath, tableName, true, "", skip, limit, spark);
    }

    @Override
    protected void readDataSpark(SparkSession spark) throws IOException {
        this.df = spark.read().option("multiline","true").json(this.filePath.toString());
    }
}
