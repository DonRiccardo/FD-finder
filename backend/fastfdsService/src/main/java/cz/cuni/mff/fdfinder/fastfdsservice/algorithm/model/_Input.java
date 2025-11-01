package cz.cuni.mff.fdfinder.fastfdsservice.algorithm.model;

import com.google.common.collect.ImmutableList;
import de.metanome.algorithms.fastfds.fastfds_helper.modules.container._FunctionalDependency;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

/**
 * {@link _Input} is a class for storing data about dataset, read and process dataset.
 * Also stores List of found FDs in the dataset.
 */
public abstract class _Input implements Serializable {

    protected String filePath;

    protected Dataset<Row> df;
    protected JavaRDD rdd;

    protected boolean hasHeader;
    protected String fileName;
    protected String tableName;

    protected int numberOfColumns;
    protected long numberOfRows;
    protected ImmutableList<String> names;
    protected String delimiter;
    protected int skip;
    protected int limit;

    protected List<_FunctionalDependency> foundFds;


    public _Input(String filePath, String tableName, boolean hasHeader, String delim, int skip,
                  int limit, SparkSession spark) {

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
        this.foundFds = new LinkedList<>();

        this.rdd = df.rdd().zipWithIndex().toJavaRDD();
    }

    abstract protected void readDataSpark(SparkSession  spark) ;

    /**
     * Skip {@code skip} lines and keep {@code limit} number of lines.
     */
    private void prepareData() {

        if (this.skip > 0) {

            this.df = this.df.offset(this.skip);
        }
        if (this.limit > 0) {

            this.df = this.df.limit(this.limit);
        }
    }

    /**
     * Gets and store columns names of the dataset.
     */
    private void getNames() {

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

    /**
     * Calculate the number of columns in the dataset and number of attributes.
     */
    private void calcNumbers() {

        this.numberOfColumns = this.df.columns().length;
        this.numberOfRows = df.count();
    }

    /**
     * Get data separated by rows and delimiter into {@link String} arrays.
     * @return {@link List} of {@link String} arrays
     */
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

    /**
     *
     * @return read data as {@link JavaRDD}
     */
    public JavaRDD<Tuple2<Row, Long>> getRddData(){

        return this.rdd;
    }

    /**
     *
     * @return {@link ImmutableList} of {@link String} column names of the dataset
     */
    public ImmutableList<String> columnNames() {

        return this.names;
    }

    /**
     *
     * @return {@link Integer} number of columns in the dataset
     */
    public int numberOfColumns() {

        return this.numberOfColumns;
    }

    /**
     *
     * @return {@link Long} number of rows in the dataset
     */
    public long numberOfRows(){

        return this.numberOfRows;
    }

    /**
     * Get name of the dataset, which is usually name of the file.
     * @return {@link String} name of the dataset
     */
    public String relationName() {

        return this.tableName;
    }

    /**
     * Generate new copy of the {@link _Input}
     * @return new copy
     * @throws Exception
     */
    public _Input generateNewCopy() throws Exception {

        return this;
    }

    /**
     * Store found FD in the list.
     * @param fd found {@link _FunctionalDependency}
     */
    public void receiveResult(_FunctionalDependency fd) {

        foundFds.add(fd);
    }

    /**
     *
     * @return List of found {@link _FunctionalDependency}
     */
    public List<_FunctionalDependency> getFoundFds(){

        return foundFds;
    }

    /**
     * Returns dataset in the {@link List} form of {@link Tuple2}(Row, index).
     * @return dataset data as {@link List}
     */
    public List<Tuple2<Row, Long>> getDataAsListAndIndex() {

        return this.rdd.collect();
    }
}
