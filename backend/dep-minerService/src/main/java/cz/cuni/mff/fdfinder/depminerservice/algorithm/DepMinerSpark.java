package cz.cuni.mff.fdfinder.depminerservice.algorithm;

import cz.cuni.mff.fdfinder.depminerservice.algorithm.model._CSVInput;
import cz.cuni.mff.fdfinder.depminerservice.algorithm.model._FunctionalDependency;
import cz.cuni.mff.fdfinder.depminerservice.algorithm.model._Input;
import cz.cuni.mff.fdfinder.depminerservice.algorithm.model._JSONInput;
import cz.cuni.mff.fdfinder.depminerservice.model.FileFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class initializing algorithm execution.
 * @author Richard
 */
public class DepMinerSpark {

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

	public static _Input input;

	public DepMinerSpark(JavaSparkContext context, SparkSession spark, Path filePath, String tableName,
						 int skip, int limit, int maxLhs, FileFormat fileFormat, boolean header, String delim)
			throws IOException {

		DepMinerSpark.context = context;
		DepMinerSpark.spark = spark;
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
	 * @return {@link HashSet} of ({@link _FunctionalDependency}) founded in the dataset
	 * @throws Exception from {@link cz.cuni.mff.fdfinder.depminerservice.algorithm.service._FunctionalDependencyGenerator}
	 */
	public List<_FunctionalDependency> startAlgorithm() throws Exception {

		long startTime = System.currentTimeMillis();
		System.out.println("START Spark: " + startTime);

		DepMinerSparkAlgorithm main = new DepMinerSparkAlgorithm(this.input, this.maxLhs);
		main.execute();

		long stopTime = System.currentTimeMillis();
		long time = stopTime - startTime;
		System.out.println("Time: " + time);

		return input.getFoundFds();

	}

}
