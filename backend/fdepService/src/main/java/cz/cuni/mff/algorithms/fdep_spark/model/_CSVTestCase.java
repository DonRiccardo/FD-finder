/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package cz.cuni.mff.algorithms.fdep_spark.model;

import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.BitSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author Richard
 */
public class _CSVTestCase implements Serializable{
    
    private String filePath;

    private Dataset<Row> df;

    private boolean hasHeader;
    private String fileName;
    private String tableName;

    private int numberOfColumns;
    private long numberOfRows;
    private ImmutableList<String> names;
    private String delimiter;
    private int skip;
    private int limit;

    private List<_FunctionalDependency> foundFds;


    public _CSVTestCase(String filePath, String tableName, boolean hasHeader, String delim, int skip, int limit, SparkSession spark) throws IOException {

        this.filePath = filePath;
        Path p = Paths.get(this.filePath);
        this.fileName = p.getFileName().toString();
        this.tableName = tableName;
        this.hasHeader = hasHeader;
        this.delimiter = delim;
        this.skip = skip;
        this.limit = limit;
        //this.outputFile = pathToOutputFile+this.fileName+"-FDs-"+spark.sparkContext().appName();

        //this.fileReader = new FileReader(new File(this.filePath));       
        //this.buffReader = new BufferedReader(new FileReader(new File(this.filePath)));
        this.df = spark.read().option("header", this.hasHeader).option("delimiter", this.delimiter).csv(this.filePath.toString());
        prepareData();

        this.calcNumbers();
        this.getNames();
        this.foundFds = new LinkedList<>();
        
        //this.createOutputFile();
        
    }

    /*
    private void setDelimiter() throws IOException{
        
        BufferedReader br = new BufferedReader(new FileReader(new File(this.filePath.toString())));
        String nextLine = br.readLine();

        if (nextLine.split(",", -1).length > nextLine.split(";", -1).length) {
            this.delimiter = ",";
        } else {
            this.delimiter = ";";
        }   
        
        br.close();
        br = null;
        nextLine = null;
    }
    */

    private void prepareData() {

        if (this.skip >= 0) {

            this.df = this.df.offset(this.skip);
        }
        if (this.limit > 0) {

            this.df = this.df.limit(this.limit);
        }
    }

    /*
    public static List<String> getAllFileNames() {

        File[] fa = new File(_CSVTestCase.pathToFiles).listFiles();

        List<String> result = new LinkedList<>();
        for (File f : fa) {

            if (f.getName().contains(".csv")) {
                result.add(f.getName());
            }

        }

        return result;

    }
    */
    /*
    public static void writeToResultFile(String s) throws IOException {

        bw.write(s);
        bw.newLine();
        bw.flush();
    }
    
    public static void init() throws IOException {

        _CSVTestCase.bw = new BufferedWriter(new FileWriter("Result" + System.currentTimeMillis() + ".csv"));
        bw.write("file;time;mem");
        bw.newLine();
        bw.flush();
    }

    public void close() throws IOException {

        _CSVTestCase.bw.close();
    }
    */
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

    public _CSVTestCase generateNewCopy() throws Exception {

        return this;
    }


    public void receiveResult(_FunctionalDependency fd) {

        foundFds.add(fd);
        // System.out.println(fd.getDeterminant() + "-->" + fd.getDependant());
    }

    public List<_FunctionalDependency> getFoundFds(){

        return foundFds;
    }


    /*
    private void createOutputFile(){
        try {
            File myObj = new File(outputFile);
            if (myObj.createNewFile()) {
              System.out.println("Output file created: " + myObj.getName());
            } else {
                System.out.println("Output file already exists. Deleting file data.");              
                FileWriter myWriter = new FileWriter(outputFile, false);
                myWriter.write("");
                myWriter.close();   
            }
        } catch (IOException e) {
            System.out.println("An error occurred while creating outputFile.");
            
        }
        
    }

     */
    /*
    public void printResultFile(HashSet<Tuple2<BitSet, Integer>> resultFDs){
        try {
            FileWriter myWriter = new FileWriter(outputFile);
            for (Tuple2<BitSet, Integer> fd : resultFDs){
                myWriter.write(fd._1+" -> "+fd._2 + System.getProperty("line.separator"));
                System.out.println(fd._1+" -> "+fd._2);
            }
            myWriter.close();
            System.out.println("Successfully wrote to the output file.");
        } catch (IOException e) {
            System.out.println("An error occurred while printing results.");
            
        }
    }


     */
    /*
    public void addToResultFile(_FunctionalDependency fd){
        try {
            FileWriter myWriter = new FileWriter(outputFile, true);
            myWriter.write(fd.toString()+ System.getProperty("line.separator"));
            myWriter.close();
            System.out.println("Successfully wrote to the output file.");
        } catch (IOException e) {
            System.out.println("An error occurred while printing results.");
            
        }
    }

     */
}
