package cz.cuni.mff.algorithms.fdep_spark;


import cz.cuni.mff.algorithms.fdep_spark.model._CSVTestCase;
import cz.cuni.mff.algorithms.fdep_spark.model._FDTree;
import cz.cuni.mff.algorithms.fdep_spark.model._FDTreeElement;
import cz.cuni.mff.algorithms.fdep_spark.model._FunctionalDependency;
import cz.cuni.mff.algorithms.fdep_spark.model._FunctionalDependency._ColumnCombination;
import cz.cuni.mff.algorithms.fdep_spark.model._FunctionalDependency._ColumnIdentifier;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;

public class FdepSparkAlgorithm implements Serializable{

    //public static final String INPUT_SQL_CONNECTION = "DatabaseConnection";
    //public static final String INPUT_TABLE_NAME = "Table_Name";
    //public static final String INPUT_TAG = "Relational Input";
    private static final int GROUP_CAPACITY = 2000;
    
    private final _CSVTestCase input;
    private static JavaSparkContext context;

    //private DatabaseConnectionGenerator databaseConnectionGenerator;
    //private RelationalInputGenerator relationalInputGenerator;
    private String tableName;
    private List<String> columnNames;
    private ObjectArrayList<_ColumnIdentifier> columnIdentifiers;

    private int numberAttributes;

    private static _FDTree negCoverTree;
    private static _FDTree posCoverTree;
    private ObjectArrayList<List<String>> tuples;
    private ObjectArrayList<ObjectArrayList<String[]>> groups;
    

    //private FunctionalDependencyResultReceiver fdResultReceiver;
    
    public FdepSparkAlgorithm (_CSVTestCase input, JavaSparkContext context){
        this.input = input;
        this.context = context;
        
        this.groups = new ObjectArrayList<>();
    }

/*
    public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
        ArrayList<ConfigurationRequirement<?>> requiredConfig = new ArrayList<>();
//		requiredConfig.add(new ConfigurationSpecificationSQLIterator(
//			INPUT_SQL_CONNECTION));
//		requiredConfig.add(new ConfigurationSpecificationString(INPUT_TABLE_NAME));
        requiredConfig.add(new ConfigurationRequirementRelationalInput(INPUT_TAG));

        return requiredConfig;
    }
*/
   /*
    public void setStringConfigurationValue(String identifier, String... values) {
        if (identifier.equals(INPUT_TABLE_NAME)) {
            this.tableName = values[0];
        }
    }
*/
/*
    public void setRelationalInputConfigurationValue(String identifier, RelationalInputGenerator... values) {
        if (identifier.equals(INPUT_TAG)) {
            this.relationalInputGenerator = values[0];
        }
    }
*/
/*
    public void setResultReceiver(
            FunctionalDependencyResultReceiver resultReceiver) {
        fdResultReceiver = resultReceiver;
    }

    */
    public void execute() {

        System.out.println("START Spark EXECUTE ");

        initialize();
        negativeCover();
        this.tuples = null;

        posCoverTree = new _FDTree(numberAttributes);
        posCoverTree.addMostGeneralDependencies();
        BitSet activePath = new BitSet();
        calculatePositiveCover(negCoverTree, activePath);
//		posCoverTree.filterGeneralizations();
        addAllDependenciesToResultReceiver();
    }

    private void initialize() {
        loadData();
        setColumnIdentifiers();
    }

    /**
     * Calculate a set of fds, which do not cover the invalid dependency lhs -> a.
     */
    private void specializePositiveCover(BitSet lhs, int a) {
        BitSet specLhs = new BitSet();

        while (posCoverTree.getGeneralizationAndDelete(lhs, a, 0, specLhs)) {
            for (int attr = this.numberAttributes; attr > 0; attr--) {
                if (!lhs.get(attr) && (attr != a)) {
                    specLhs.set(attr);
                    if (!posCoverTree.containsGeneralization(specLhs, a, 0)) {
                        posCoverTree.addFunctionalDependency(specLhs, a);
                    }
                    specLhs.clear(attr);
                }
            }
            specLhs = new BitSet();
        }
    }

    private void calculatePositiveCover(_FDTreeElement negCoverSubtree, BitSet activePath) {
        for (int attr = 1; attr <= numberAttributes; attr++) {
            if (negCoverSubtree.isFd(attr - 1)) {
                specializePositiveCover(activePath, attr);
            }
        }

        for (int attr = 1; attr <= numberAttributes; attr++) {
            if (negCoverSubtree.getChild(attr - 1) != null) {
                activePath.set(attr);
                this.calculatePositiveCover(negCoverSubtree.getChild(attr - 1), activePath);
                activePath.clear(attr);
            }
        }
    }


    /**
     * Calculate the negative Cover for the current relation.
     */
    private void negativeCover() {
        negCoverTree = new _FDTree(this.numberAttributes);
        /*
        for (int i = 0; i < tuples.size(); i++) {
            for (int j = i + 1; j < tuples.size(); j++) {
                violatedFds(tuples.get(i), tuples.get(j));
            }
        }
        */
        LinkedList<Tuple2<ObjectArrayList<String[]>, ObjectArrayList<String[]>>> pair = new LinkedList<>();
        
        for (int i = 0; i < this.groups.size() - 1; i++) {
            for (int j = i + 1; j < this.groups.size(); j++) {
                pair.add(new Tuple2<>(this.groups.get(i), this.groups.get(j)));
            }
            pair.add(new Tuple2<>(this.groups.get(i), null));
        }
        
        pair.add(new Tuple2<>(this.groups.get(this.groups.size()-1), null));
        
        JavaRDD<Tuple2<ObjectArrayList<String[]>, ObjectArrayList<String[]>>> groupsParalel = this.context.parallelize(pair);
        groupsParalel
                .foreach(tuple -> {
                    
                    if (tuple._2 == null){
                        for (int i = 0; i < tuple._1.size() -1 ; i++) {
                            for (int j = i + 1; j < tuple._1.size(); j++) {
                                violatedFds(tuple._1.get(i), tuple._1.get(j));
                            }
                        }
                    }
                    else {
                        for (int i = 0; i < tuple._1.size(); i++) {
                            for (int j = 0; j < tuple._2.size(); j++) {
                                violatedFds(tuple._1.get(i), tuple._2.get(j));
                            }
                        }
                    }
                });
        
        
        this.negCoverTree.filterSpecializations();
    }


    /**
     * Find the least general functional dependencies violated by t1 and t2
     * and add update the negative cover accordingly.<br/>
     * Note: t1 and t2 must have the same length.
     *
     * @param t1 An ObjectArrayList with the values of one entry of the relation.
     * @param t2 An ObjectArrayList with the values of another entry of the relation.
     */
    private void violatedFds(List<String> t1, List<String> t2) {
        BitSet equalAttr = new BitSet();
        equalAttr.set(1, this.numberAttributes + 1);
        BitSet diffAttr = new BitSet();
        for (int i = 0; i < t1.size(); i++) {
            Object val1 = t1.get(i);
            Object val2 = t2.get(i);
            // Handling of null values. Currently assuming NULL values are equal.
            if (val1 == null && val2 == null) {
                continue;
            } else if ((val1 == null && val2 != null) || !(val1.equals(val2))) {
                // BitSet start with 1 for first attribute
                diffAttr.set(i + 1);
            }
        }
        equalAttr.andNot(diffAttr);
        for (int a = diffAttr.nextSetBit(0); a >= 0; a = diffAttr.nextSetBit(a + 1)) {
            negCoverTree.addFunctionalDependency(equalAttr, a);
        }
    }
    
    /**
     * Find the least general functional dependencies violated by t1 and t2
     * and add update the negative cover accordingly.<br/>
     * Note: t1 and t2 must have the same length.
     *
     * @param t1 An ObjectArrayList with the values of one entry of the relation.
     * @param t2 An ObjectArrayList with the values of another entry of the relation.
     */
    private void violatedFds(String[] t1, String[] t2) {
        BitSet equalAttr = new BitSet();
        equalAttr.set(1, this.numberAttributes + 1);
        BitSet diffAttr = new BitSet();
        for (int i = 0; i < t1.length; i++) {
            Object val1 = t1[i];
            Object val2 = t2[i];
            // Handling of null values. Currently assuming NULL values are equal.
            if (val1 == null && val2 == null) {
                continue;
            } else if ((val1 == null && val2 != null) || !(val1.equals(val2))) {
                // BitSet start with 1 for first attribute
                diffAttr.set(i + 1);
            }
        }
        equalAttr.andNot(diffAttr);
        for (int a = diffAttr.nextSetBit(0); a >= 0; a = diffAttr.nextSetBit(a + 1)) {
            negCoverTree.addFunctionalDependency(equalAttr, a);
        }
    }


    /**
     * Fetch the data from the database and keep it as List of Lists.
     *
     * //@throws AlgorithmExecutionException
     * //@throws AlgorithmConfigurationException
     */
    private void loadData() {
        tuples = new ObjectArrayList<List<String>>();
        /*
        RelationalInput ri = null;
        
        if (this.relationalInputGenerator != null) {
            ri = this.relationalInputGenerator.generateNewCopy();
        } else if (this.databaseConnectionGenerator != null && this.tableName != null) {
            String sql = "SELECT * FROM " + this.tableName;
            ri = this.databaseConnectionGenerator.generateRelationalInputFromSql(sql, this.tableName);
        } else {
            throw new AlgorithmConfigurationException("No input Generator set.");
        }
        */
        this.columnNames = this.input.columnNames();
        this.tableName = this.input.relationName();
        this.numberAttributes = this.input.numberOfColumns();
        
        /*
        ObjectArrayList group = new ObjectArrayList();
        String[] line = this.input.getNextData();
        
        while (line != null) {
           group.add(line);
           tuples.add(Arrays.asList(line));
           if (group.size() >= FdepSparkAlgorithm.GROUP_CAPACITY){
               groups.add(group);
               group = new ObjectArrayList();
           }
           
           line = this.input.getNextData();
        }
        */
        
        
        List<String[]> data = this.input.getData();
        
        int numOfGroups = data.size() / FdepSparkAlgorithm.GROUP_CAPACITY;
        //System.out.println("NUM_GROUPS: "+numOfGroups);
        for (int i = 0; i < numOfGroups; i++) {
            ObjectArrayList<String[]> group = new ObjectArrayList();
            group.addAll(data.subList(i*FdepSparkAlgorithm.GROUP_CAPACITY,(i+1)*FdepSparkAlgorithm.GROUP_CAPACITY));
            /*
            for (int j = 0; j < group.size(); j++) {
                System.out.println("group memeber: "+Arrays.toString(group.get(j)));
            }
            */
            this.groups.add(group);
        }
        
        ObjectArrayList<String[]> group = new ObjectArrayList();
        group.addAll(data.subList(numOfGroups*FdepSparkAlgorithm.GROUP_CAPACITY,data.size()));
        this.groups.add(group);
        /*
        for (int j = 0; j < group.size(); j++) {
                System.out.println("group memeber: "+Arrays.toString(group.get(j)));
            }
        */
        
    }

    private void setColumnIdentifiers() {
        this.columnIdentifiers = new ObjectArrayList<_ColumnIdentifier>(
                this.columnNames.size());
        for (String column_name : this.columnNames) {
            columnIdentifiers.add(new _ColumnIdentifier(this.tableName,
                    column_name));
        }
    }

    
    private void addAllDependenciesToResultReceiver(_FDTreeElement fds, BitSet activePath) {
        /*
        if (this.fdResultReceiver == null) {
           // return;
        }
        */
        for (int attr = 1; attr <= numberAttributes; attr++) {
            if (fds.isFd(attr - 1)) {
                int j = 0;
                _ColumnIdentifier[] columns = new _ColumnIdentifier[activePath.cardinality()];
                for (int i = activePath.nextSetBit(0); i >= 0; i = activePath.nextSetBit(i + 1)) {
                    columns[j++] = this.columnIdentifiers.get(i - 1);
                }
                
                _ColumnCombination colCombination = new _ColumnCombination(columns);
                _FunctionalDependency fdResult = new _FunctionalDependency(colCombination, columnIdentifiers.get(attr - 1));
				System.out.println(fdResult.toString());
                                //this.input.addToResultFile(fdResult);
                // TODO zapis vysledkov do listu?

                //fdResultReceiver.receiveResult(fdResult);
            }
        }

        for (int attr = 1; attr <= numberAttributes; attr++) {
            if (fds.getChild(attr - 1) != null) {
                activePath.set(attr);
                this.addAllDependenciesToResultReceiver(fds.getChild(attr - 1), activePath);
                activePath.clear(attr);
            }
        }
    }


    /**
     * Add all functional Dependencies to the FunctionalDependencyResultReceiver.
     * Do nothing if the object does not have a result receiver.
     *
     * //@throws CouldNotReceiveResultException
     * //@throws ColumnNameMismatchException
     */
    private void addAllDependenciesToResultReceiver() {
        /*
        if (this.fdResultReceiver == null) {
          //  return;
        }
        */
        this.addAllDependenciesToResultReceiver(posCoverTree, new BitSet());
    }


}
