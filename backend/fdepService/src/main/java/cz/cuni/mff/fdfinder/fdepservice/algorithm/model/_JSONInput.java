package cz.cuni.mff.fdfinder.fdepservice.algorithm.model;

import org.apache.spark.sql.SparkSession;

/**
 * Version of {@link _Input} which reads JSON file.
 */
public class _JSONInput extends _Input{

    public _JSONInput(String filePath, String tableName, int skip, int limit, SparkSession spark) {
        super(filePath, tableName, true, "", skip, limit, spark);
    }

    @Override
    protected void readDataSpark(SparkSession spark) {
        this.df = spark.read().option("multiline","true").json(this.filePath.toString());
    }
}
