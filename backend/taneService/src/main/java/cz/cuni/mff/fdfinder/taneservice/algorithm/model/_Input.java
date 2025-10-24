package cz.cuni.mff.fdfinder.taneservice.algorithm.model;

import com.google.common.collect.ImmutableList;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

public abstract class _Input implements Serializable {

    protected String filePath;

    protected Dataset<Row> df;
    protected JavaRDD rddData;

    protected boolean hasHeader;
    protected String fileName;
    protected String tableName;

    protected int numberOfColumns;
    protected long numberOfRows;
    protected ImmutableList<String> names;
    protected String delimiter;
    protected int skip;
    protected int limit;

    protected HashSet<_FunctionalDependency> foundFds;


    public _Input(String filePath, String tableName, boolean hasHeader, String delim, int skip,
                  int limit, SparkSession spark) throws IOException {

        this.filePath = filePath;
        Path p = Paths.get(this.filePath);
        this.fileName = p.getFileName().toString();
        this.tableName = tableName;
        this.hasHeader = hasHeader;
        this.delimiter = delim;
        this.skip = skip;
        this.limit = limit;

        readDataSpark(spark);
        prepareData();

        this.calcNumbers();
        this.getNames();
        this.foundFds = new HashSet<>();

        this.rddData = df.rdd().zipWithIndex().toJavaRDD();

    }

    abstract protected void readDataSpark(SparkSession  spark) throws IOException;

    private void prepareData() {

        if (this.skip > 0) {

            this.df = this.df.offset(this.skip);
        }
        if (this.limit > 0) {

            this.df = this.df.limit(this.limit);
        }
    }

    private void getNames() throws IOException {

        ImmutableList.Builder<String> builder = new ImmutableList.Builder<>();

        if (this.hasHeader) {

            for (String s : this.df.columns()) {
                builder.add(s);
            }
        }
        else {

            for (int i = 0; i < this.numberOfColumns; i++) {

                builder.add(this.tableName + ":" + i);
            }
        }
        this.names = builder.build();

    }

    private void calcNumbers() {

        this.numberOfColumns = this.df.columns().length;
        this.numberOfRows = df.count();
    }

    public List<String[]> getData(){
        return this.df
                .map((MapFunction<Row, String[]>) row -> {
                    String[] values = new String[this.numberOfColumns];
                    for (int i = 0; i < this.numberOfColumns; i++) {
                        Object value = row.get(i);
                        values[i] = value == null ? null : value.toString();
                    }
                    //System.out.println("DATA: "+Arrays.toString(values));
                    return values;
                }, Encoders.javaSerialization(String[].class))
                .collectAsList();

    }

    public JavaRDD<Tuple2<Row, Long>> getRddData(){

        return this.rddData;
    }

    public ImmutableList<String> columnNames() {

        return this.names;
    }

    public int numberOfColumns() {

        return this.numberOfColumns;
    }

    public long numberOfRows(){

        return this.numberOfRows;
    }

    public String relationName() {

        return this.tableName;
    }

    public _Input generateNewCopy() throws Exception {

        return this;
    }

    public void receiveResult(_FunctionalDependency fd) {

        foundFds.add(fd);
        // System.out.println(fd.getDeterminant() + "->" + fd.getDependant());
    }

    public HashSet<_FunctionalDependency> getFoundFds(){

        return foundFds;
    }

}
