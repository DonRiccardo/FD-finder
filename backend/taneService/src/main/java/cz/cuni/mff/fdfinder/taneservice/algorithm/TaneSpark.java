package cz.cuni.mff.fdfinder.taneservice.algorithm;

import cz.cuni.mff.fdfinder.taneservice.algorithm.model._CSVInput;
import de.metanome.algorithms.depminer.depminer_helper.modules.container._FunctionalDependency;
import cz.cuni.mff.fdfinder.taneservice.algorithm.model._Input;
import cz.cuni.mff.fdfinder.taneservice.algorithm.model._JSONInput;
import cz.cuni.mff.fdfinder.taneservice.model.FileFormat;
import de.metanome.algorithms.tane.TaneSparkAlgorithm;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;

/**
 * Class initializing algorithm execution.
 * @author Richard
 */
public class TaneSpark {

    private static JavaSparkContext context = null;
    private static SparkSession spark = null;
    private final Path filePath;
    private final String tableName;
    private final int skip;
    private final int limit;
    private final int maxLhs;
    private final FileFormat fileFormat;
    private final boolean header;
    private final String delim;

    private static _Input input;

    public TaneSpark(JavaSparkContext context, SparkSession spark, Path filePath, String tableName,
                      int skip, int limit, int maxLhs, FileFormat fileFormat, boolean header, String delim) throws IOException {

        TaneSpark.context = context;
        TaneSpark.spark = spark;
        this.filePath = filePath;
        this.tableName = tableName;
        this.skip = skip;
        this.limit = limit;
        this.maxLhs = maxLhs;
        this.fileFormat = fileFormat;
        this.header = header;

        if ((delim == null || delim.isEmpty()) && fileFormat == FileFormat.CSV) {

            this.delim = ",";
        }
        else this.delim = delim;

        loadInput();
    }

    /**
     * Reads dataset into {@link _Input} and prepare it for execution.
     * @throws UnsupportedOperationException if the dataset format is not supported
     */
    private void loadInput() throws UnsupportedOperationException {
        System.out.println("SPARK starting to create INPUT");

        if (fileFormat == FileFormat.CSV) {

            input = new _CSVInput(filePath.toString(), tableName, header, delim, skip, limit, spark);
            System.out.println("SPARK starting to create _CSV_INPUT");
        }
        else if (fileFormat == FileFormat.JSON) {

            input = new _JSONInput(filePath.toString(), tableName, skip, limit, spark);
        }
        else {
            System.out.println("SPARK INPUT problem!");
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }

    /**
     * Sets and starts algorithm execution.
     * @return {@link HashSet} of ({@link _FunctionalDependency}) founded in the datasetň
     */
    public HashSet<_FunctionalDependency> startAlgorithm() {

        long startTime = System.currentTimeMillis();
        System.out.println("START Spark: " + startTime);

        TaneSparkAlgorithm main = new TaneSparkAlgorithm(input, maxLhs, context);
        main.execute();

        long stopTime = System.currentTimeMillis();
        long time = stopTime - startTime;
        System.out.println("Time: " + time);

        System.out.println("FINISHED Spark FD #: " + input.getFoundFds().size());

        return input.getFoundFds();

    }
    
    
}
