/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package cz.cuni.mff.algorithms.fdep_spark;

import cz.cuni.mff.algorithms.fdep_spark.model._CSVTestCase;
import cz.cuni.mff.fdfinder.fdepservice.FileFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Richard
 */
public class FdepSpark {
    
    //public static String FILENAME;
    public static boolean hasHeader;
    public static String delim;

    private static SparkConf conf = new SparkConf();
    private static JavaSparkContext context = null;
    private static SparkSession spark = null;
    
    public void startAlgorithm(Path filePath, int skip, int limit, int maxLhs, FileFormat fileFormat, boolean header, String delim) {
        
//        FILENAME = "../datasets/imdb-movies.csv"; hasHeader = true;
//        FILENAME = "../datasets/test-example.csv"; hasHeader = true;
//        FILENAME = "../datasets/breast.csv"; hasHeader = true;
//        FILENAME = "../datasets/breastx16.csv"; hasHeader = true;
//        FILENAME = "../datasets/breastx64.csv"; hasHeader = true;
//        FILENAME = "../datasets/breast-newx79.csv"; hasHeader = true;
//        FILENAME = "../datasets/abalone.csv"; hasHeader = true;
        
        try {

            // Application name to show on the cluster UI
            conf.setAppName("FDep-Spark");
            // cluster URL (spark://ip_address:7077) or string "local" to run in local mode
            conf.setMaster("local");

            // Context tells Spark how to access a cluster
            context = new JavaSparkContext(conf);

            spark = SparkSession.builder().appName("FDep-Spark").getOrCreate();
            _CSVTestCase input;

            System.out.println("SPARK starting to create INPUT");

//			int numberOfThreads = 1;
            if (fileFormat == FileFormat.CSV) {
                if (delim == null || delim.isEmpty()) {

                    delim = ",";
                }

                 input = new _CSVTestCase(filePath.toString(), header, delim, skip, limit, spark);
                System.out.println("SPARK starting to create _CSV_INPUT");
            }
            else {
                System.out.println("SPARK INPUT problem!");
                throw new UnsupportedOperationException("Not supported yet.");
            }

            //System.out.println(input.getData().collect());
            //System.out.println("HEADER: "+input.columnNames());

            long startTime = System.currentTimeMillis();
			System.out.println("START Spark: " + startTime);

            FdepSparkAlgorithm main = new FdepSparkAlgorithm(input, context);
            main.execute();

            long stopTime = System.currentTimeMillis();
            long time = stopTime - startTime;
            System.out.println("Time: " + time);

//			if (FILENAME.equals("breast_proj.csv")) {
//				main.demo();
//			}
//			
//			if (FILENAME.equals("titanic.csv")) {
//				main.demo2();
//			}


            spark.stop();
            context.stop();

        } catch (Exception ex) {
            spark.stop();
            context.stop();
            Logger.getLogger(FdepSpark.class.getName()).log(Level.SEVERE, "Something went wrong.", ex);
        }
    }
}
