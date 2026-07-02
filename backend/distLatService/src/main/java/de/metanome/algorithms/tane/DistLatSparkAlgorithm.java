package de.metanome.algorithms.tane;


import cz.cuni.mff.fdfinder.distlatservice.algorithm.model.*;
import de.metanome.algorithms.depminer.depminer_helper.modules._StrippedPartitionGenerator;
import de.metanome.algorithms.depminer.depminer_helper.modules.container._FunctionalDependencyGroup;
import de.metanome.algorithms.tane.model.*;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.longs.LongList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Class representing actual working algorithm.
 */
public class DistLatSparkAlgorithm implements Serializable{
    
    private static _Input input;
    private static JavaSparkContext context;
    private final int maxLhs;
    private final int startLatticeLevel;
    /**
     * How fast are we going up in the lattice.
     * <br/>
     * <strong>Example:</strong> <br/>
     * actual LVL = 2 <br/>
     * {@code skippingLvls} = 1 <br/>
     * next LVL = 3
     */
    private final int skippingLvls;

    private int numberAttributes;
    private long numberTuples;

    private Map<BitSet, List<BitSet>> prefix_blocks = null;
    /**
     * Map with stripped partitions for single attributes
     */
    private Map<Integer, _StrippedPartitionSpark> plisMap = null;
    private JavaPairRDD<BitSet, CombinationHelperSpark> latticeLevel = null;

    private Set<BitSet> attributeSet = null;
    /**
     * Last generated {@link _StrippedPartitionSpark} in the previous lattice level.
     * LVL with attributes of size 2 was already checked.
     * If lattice level contained attributes of size 2, this Map contains attributes of size 3.
     * <br/>
     * <strong>Example</strong> LVL: AB, generated {@link _StrippedPartitionSpark} for ABC (FD AB->C was checked) => stored partition of ABC
     */
    private Map<BitSet, _StrippedPartitionSpark> lastGeneratedPartitions = null;
    /**
     * New generated {@link _StrippedPartitionSpark} in the actual lattice level.
     * If lattice level contains attributes of size 2, this Map contains attributes of size 3.
     * <br/>
     * <strong>Example</strong> LVL: AB, generated {@link _StrippedPartitionSpark} for ABC (FD AB->C was checked) => store partition of ABC
     */
    private Map<BitSet, _StrippedPartitionSpark> newGeneratedPartitions = null;

    private static Map<BitSet, _StrippedPartitionSpark> computedPartitions = null;
    
    public DistLatSparkAlgorithm(_Input input, int maxLhs, JavaSparkContext context){
        DistLatSparkAlgorithm.input = input;
        if (maxLhs < 0) {

            this.maxLhs = (DistLatSparkAlgorithm.input.numberOfColumns() > 10)
                    ? (int) Math.ceil(input.numberOfColumns()*0.7)
                    : input.numberOfColumns();
        }
        else {

            this.maxLhs = Math.min(maxLhs, DistLatSparkAlgorithm.input.numberOfColumns());
        }

        if (DistLatSparkAlgorithm.input.numberOfColumns() < 7){
            this.startLatticeLevel = 0;
            this.skippingLvls = 1;
        }
        else {
            this.startLatticeLevel = 3;
            this.skippingLvls = 3;
        }

        DistLatSparkAlgorithm.context = context;
    }

    /**
     * Run algorithm
     */
    public void execute() {
        
        loadData();

        ArrayList<Tuple2<BitSet, CombinationHelperSpark>> listLVL1 = new ArrayList<>();
        for (BitSet bs : attributeSet){

            listLVL1.add(new Tuple2<>(bs, new CombinationHelperSpark(bs, this.numberAttributes)));
        }

        latticeLevel = context.parallelizePairs(listLVL1);

        // going up in the lattice
        int l = startLatticeLevel;
        int canSkipLvls = (l + this.skippingLvls >= maxLhs) ? 1 : this.skippingLvls;

        while (latticeLevel.count() > 0 && l < numberAttributes && l <= maxLhs){
            //System.out.println("\n LATTICE LVL: "+ l);

            lastGeneratedPartitions = newGeneratedPartitions;
            newGeneratedPartitions = new HashMap<>();
            computedPartitions = new  HashMap<>();
            computeLatticeLevel(canSkipLvls);

            l += canSkipLvls;
            canSkipLvls = (l + this.skippingLvls >= maxLhs) ? 1 : this.skippingLvls;

        }

    }

    /**
     * Load data and create {@link _StrippedPartitionSpark} and initialize variables.
     */
    private void loadData() {

        this.prefix_blocks = new HashMap<>();
        this.numberAttributes = input.numberOfColumns();
        this.numberTuples = input.numberOfRows();
        this.attributeSet = new HashSet<>();
        this.lastGeneratedPartitions = new HashMap<>();
        //this.lastGeneratedPartitions.put(new BitSet(), new _StrippedPartitionSpark(this.numberTuples));
        this.newGeneratedPartitions = new HashMap<>();
        this.newGeneratedPartitions.put(new BitSet(), new _StrippedPartitionSpark(this.numberTuples));
        DistLatSparkAlgorithm.computedPartitions = new HashMap<>();

        if (startLatticeLevel > 0){
            // create combinations of attributes for specified LVL
            for (int i = 0; i < this.numberAttributes; i++) {
                BitSet lhs = new BitSet();
                lhs.set(i);
                this.attributeSet.add(lhs);
            }

            for (int i = 1; i < startLatticeLevel; i++){
                attributeSet = generateNextLevel(attributeSet);
            }
        }
        else {
            // empty BitSet representing empty set
            // FD (empty set)->A is also possible

            attributeSet.add(new BitSet());
        }

        _StrippedPartitionGenerator spGen = new _StrippedPartitionGenerator();
        plisMap = spGen.execute(input).collectAsMap();

    }

    /**
     * Compute {@link _StrippedPartitionSpark} for specified attributes.
     * @param attributes {@link BitSet} of attributes
     * @return computed {@link _StrippedPartitionSpark}
     */
    private _StrippedPartitionSpark computeStrippedPartition(BitSet attributes){

        if (lastGeneratedPartitions.containsKey(attributes)){

            return lastGeneratedPartitions.get(attributes);
        }

        BitSet someLhs = this.lastGeneratedPartitions.keySet().iterator().next();
        BitSet bitsToMultiply = new BitSet();
        BitSet keyInMap = (BitSet) attributes.clone();

        int index =  keyInMap.nextSetBit(0);

        for (int i = 0; i < attributes.cardinality() - someLhs.cardinality(); i++){

            bitsToMultiply.set(index);
            keyInMap.clear(index);
            index = keyInMap.nextSetBit(index + 1);
        }

        _StrippedPartitionSpark spToReturn = lastGeneratedPartitions.get(keyInMap);

        for (int A = bitsToMultiply.nextSetBit(0); A >= 0; A = bitsToMultiply.nextSetBit(A + 1)) {

            spToReturn = multiply(spToReturn, plisMap.get(A));
            keyInMap.set(A);
            DistLatSparkAlgorithm.computedPartitions.put((BitSet) keyInMap.clone(), spToReturn);
        }

        return spToReturn;
    }

    private void computeLatticeLevel(int canSkipLvls) {

        newGeneratedPartitions = new HashMap<>(latticeLevel
                // compute Stripped partitions for the actual level
                .mapToPair(tuple -> {

                    _StrippedPartitionSpark newSp = computeStrippedPartition(tuple._1);
                    return new Tuple2<>(tuple._1, newSp);
                })
                .collectAsMap()
        );

        latticeLevel = latticeLevel
                // check functional dependencies and locally go down in the lattice (to find minimal FDs)
                .map(tuple -> {
                    tuple._2.setPartition(newGeneratedPartitions.get(tuple._1));
                    findFunctionalDependencies(tuple._1, tuple._2);

                    return tuple;
                })
                // generate next lattice level
                .flatMapToPair((tuple) -> {
                    Set<BitSet> combinations = new HashSet<>();
                    List<Tuple2<BitSet, CombinationHelperSpark>> listLVL = new ArrayList<>();
                    BitSet X = (BitSet) tuple._2.getLatticeBuilding().clone();

                    for (int A = X.nextSetBit(0); A >= 0; A = X.nextSetBit(A + 1)) {

                        BitSet b = new BitSet();
                        b.set(A);
                        combinations.add(b);
                    }

                    for (int i = 0; i < canSkipLvls - 1; i++) {
                        combinations = generateNextLevel(combinations);
                    }

                    for (BitSet combination : combinations) {

                        BitSet newAttributes = (BitSet) combination.clone();
                        newAttributes.or(tuple._1);
                        CombinationHelperSpark ch = new CombinationHelperSpark(newAttributes, this.numberAttributes);
                        BitSet newRhsc = (BitSet) tuple._2.getRhsCandidates().clone();
                        newRhsc.andNot(combination);
                        ch.setRhsCandidates(newRhsc);
                        listLVL.add(new Tuple2<>(newAttributes, ch));
                    }

                    return listLVL.iterator();
                })
                .reduceByKey((x, y) -> {
                    BitSet newRHSC = (BitSet) x.getRhsCandidates().clone();
                    newRHSC.or(y.getRhsCandidates());

                    x.setRhsCandidates(newRHSC);
                    return x;
                });

    }

    /**
     * Find all minimal functional dependencies from specified attributes and add them to the result.
     * @param attributes {@link BitSet} of attributes as a LHS
     * @param comhelp {@link CombinationHelperSpark} for the attributes containing RHS candidates
     */
    private void findFunctionalDependencies(BitSet attributes, CombinationHelperSpark comhelp){

        //Map<BitSet, _StrippedPartitionSpark> computedPartitions = new HashMap<>();

        //System.out.println("IN: " + attributes + " RHSC: " +  comhelp.getRhsCandidates());

        BitSet X = (BitSet) attributes.clone();
        BitSet rhsc = (BitSet) comhelp.getRhsCandidates().clone();
        rhsc.andNot(X);
        _StrippedPartitionSpark SPwithoutA = comhelp.getPartition();

        for (int A = rhsc.nextSetBit(0); A >= 0; A = rhsc.nextSetBit(A+1)) {
            BitSet XwithA = (BitSet) X.clone();
            XwithA.set(A);

            _StrippedPartitionSpark combinedSP = multiply(SPwithoutA, plisMap.get(A));
            computedPartitions.put(XwithA, combinedSP);

            if (SPwithoutA.getError() == combinedSP.getError()){
                // found a FD X->A
                //System.out.println("FOUND fd: "+ A);
                rhsc.clear(A);
                findMininalFunctionalDependencyFromOriginal(attributes, A);
            }

        }

        comhelp.setRhsCandidates(rhsc);

    }

    /**
     * From provided LHS create smaller LHS by removing one attribute. All combinations are added to {@code lhsToCHeck}
     * if they are not on the lattice level that was already checked.
     * @param lhs {@link BitSet} of attributes, the original LHS
     * @param lhsToCheck {@link Queue} where the smaller LHS are added to be checked
     */
    private void createAddOneAttributeSmallerLhsToQueue(BitSet lhs, Queue<Tuple2<BitSet, BitSet>> lhsToCheck){

        BitSet b = (BitSet)  lhs.clone();
        //b.clear(lhs.nextSetBit(0));
        if (lastGeneratedPartitions.containsKey(b)){ return;}

        for (int A = lhs.nextSetBit(0); A >= 0; A = lhs.nextSetBit(A+1)) {
            BitSet smallerLhs = (BitSet) lhs.clone();
            smallerLhs.clear(A);
            lhsToCheck.add(new Tuple2<>(smallerLhs, lhs));
        }
    }

    /**
     * Compute {@link _StrippedPartitionSpark} for specified {@link BitSet}.
     * SP is obtained from {@code computedPartitions} if was already computed otherwise is computed and added to {@code computedPartitions}.
     * @param b {@link BitSet} of which we compute SP
     * @return computed {@link _StrippedPartitionSpark}
     */
    private _StrippedPartitionSpark getStrippedPartitionFromCalculated(BitSet b){

        if (DistLatSparkAlgorithm.computedPartitions.containsKey(b)){

            return DistLatSparkAlgorithm.computedPartitions.get(b);
        }

        _StrippedPartitionSpark sp = computeStrippedPartition(b);
        DistLatSparkAlgorithm.computedPartitions.put(b, sp);
        return sp;
    }

    /**
     * From LHS->RHS as an original valid FD, find all valid minimal FDs and add them to the result.
     * @param lhs {@link BitSet} LHS of a valid FD
     * @param rhs {@link BitSet} RHS of a valid FD
     *
     */
    private void findMininalFunctionalDependencyFromOriginal(BitSet lhs, int rhs){

        Set<BitSet> lastValidFDsLHS = new HashSet<>();
        lastValidFDsLHS.add(lhs);

        Queue<Tuple2<BitSet, BitSet>> lhsToCheckForMinimality = new LinkedList<>();

        createAddOneAttributeSmallerLhsToQueue(lhs, lhsToCheckForMinimality);

        while (!lhsToCheckForMinimality.isEmpty()){

            Tuple2<BitSet, BitSet>  element = lhsToCheckForMinimality.poll();

            //System.out.println("DOWN: " + element._1 + "->" + rhs);

            BitSet XwithA = (BitSet) element._1.clone();
            XwithA.set(rhs);

            _StrippedPartitionSpark SPX = getStrippedPartitionFromCalculated(element._1);
            _StrippedPartitionSpark SPXwithA = getStrippedPartitionFromCalculated(XwithA);

            if (SPX.getError() == SPXwithA.getError()){

                lastValidFDsLHS.remove(element._2);
                if (!lastGeneratedPartitions.containsKey(element._1)) lastValidFDsLHS.add(element._1);

                createAddOneAttributeSmallerLhsToQueue(element._1, lhsToCheckForMinimality);
            }

        }

        for (BitSet b : lastValidFDsLHS){

            processFunctionalDependency(b, rhs);
        }
    }

    /**
     * Generate new {@link _StrippedPartitionSpark} which is the least refined partition that refines {@code pt1} and {@code pt2}.
     * @param pt1 {@link _StrippedPartitionSpark}
     * @param pt2 {@link _StrippedPartitionSpark}
     * @return {@link _StrippedPartitionSpark} product of inputs
     */
   public _StrippedPartitionSpark multiply(_StrippedPartitionSpark pt1, _StrippedPartitionSpark pt2) {
        LongBigArrayBigList tTable;
        tTable = new LongBigArrayBigList(numberTuples);
        for (long i = 0; i < numberTuples; i++) {
            tTable.add(-1);
        }
        List<LongList> result = new ArrayList<>();
        List<LongList> pt1List = pt1.getStrippedPartition();
        List<LongList> pt2List = pt2.getStrippedPartition();
        List<LongList> partition = new ArrayList<>();
        long noOfElements = 0;
        // iterate over first stripped partition and fill tTable.
        for (int i = 0; i < pt1List.size(); i++) {
            for (long tId : pt1List.get(i)) {
                tTable.set(tId, i);
            }
            partition.add(new LongArrayList());
        }
        // iterate over the second stripped partition.
       for (LongList longs : pt2List) {
           for (long t_id : longs) {
               // tuple is also in an equivalence class of pt1
               if (tTable.getLong(t_id) != -1) {
                   partition.get((int) tTable.getLong(t_id)).add(t_id);
               }
           }
           for (long tId : longs) {
               // if condition not in the paper;
               if (tTable.getLong(tId) != -1) {
                   if (partition.get((int) tTable.getLong(tId)).size() > 1) {
                       LongList eqClass = partition.get((int) tTable.getLong(tId));
                       result.add(eqClass);
                       noOfElements += eqClass.size();
                   }
                   partition.set((int) tTable.getLong(tId), new LongArrayList());
               }
           }
       }

        return new _StrippedPartitionSpark(result, noOfElements);
    }

    /**
     * Generates new level for finding FDs also initialize C+ for new level.
     */
    private Set<BitSet> generateNextLevel(Set<BitSet> attCombinations){
        //level0 = level1;
        //level1 = null;
        //System.gc();

        //Object2ObjectOpenHashMap<BitSet, CombinationHelperSpark> new_level = new Object2ObjectOpenHashMap<BitSet, CombinationHelperSpark>();

        Set<BitSet> newLevel = new HashSet<>();

        buildPrefixBlocks(attCombinations);

        for (List<BitSet> prefix_block_list : prefix_blocks.values()) {

            // continue only, if the prefix_block contains at least 2 elements
            if (prefix_block_list.size() < 2) {
                continue;
            }

            ArrayList<BitSet[]> combinations = getListCombinations(prefix_block_list);
            for (BitSet[] c : combinations) {
                // merge two bitsets into one -> combination
                BitSet X = (BitSet) c[0].clone();
                X.or(c[1]);
                newLevel.add(X);

            }
        }

        return newLevel;
    }

    /**
     * Get index of the last bit, that was set to {@code true}.
     * @param bitset {@link BitSet} for finding last set index
     * @return {@link Integer} index of the last set bit
     */
    private int getLastSetBitIndex(BitSet bitset) {
        int lastSetBit = 0;
        for (int A = bitset.nextSetBit(0); A >= 0; A = bitset.nextSetBit(A + 1)) {
            lastSetBit = A;
        }
        return lastSetBit;
    }
    
    /**
     * Get the prefix of BitSet by copying it and removing the last Bit.
     * @param bitset {@link BitSet} of which you get prefix
     * @return A new BitSet, where the last set Bit is cleared.
     */
    private BitSet getPrefix(BitSet bitset) {
        BitSet prefix = (BitSet) bitset.clone();
        prefix.clear(getLastSetBitIndex(prefix));
        return prefix;
    }
    
    /**
     * Build the prefix blocks for a new level. It is a HashMap containing the
     * prefix as a key and the corresponding attributes combination as the value.
     * Creating new combinations one bigger than level0 combinations
     */
    private void buildPrefixBlocks(Set<BitSet> attCombinations) {
        this.prefix_blocks.clear();

        for (BitSet level_iter : attCombinations) {
            BitSet prefix = getPrefix(level_iter);

            if (prefix_blocks.containsKey(prefix)) {
                prefix_blocks.get(prefix).add(level_iter);
            } else {
                ArrayList<BitSet> list = new ArrayList<>();
                list.add(level_iter);
                prefix_blocks.put(prefix, list);
            }
        }
    }
    
    /**
     * Get all combinations, which can be built out of the elements of a prefix block
     *
     * @param list: List of BitSets, which are in the same prefix block.
     * @return All combinations of the BitSets.
     */
    private ArrayList<BitSet[]>  getListCombinations(List<BitSet> list) {
        ArrayList<BitSet[]> combinations = new ArrayList<>();
        for (int a = 0; a < list.size(); a++) {
            for (int b = a + 1; b < list.size(); b++) {
                BitSet[] combi = new BitSet[2];
                combi[0] = list.get(a);
                combi[1] = list.get(b);
                combinations.add(combi);
            }
        }
        return combinations;
    }
    
    /**
     * Processing founf FD, adding it into a result.
     * @param XwithoutA LHS of a FD
     * @param A RHS of a FD
     */
    private void processFunctionalDependency(BitSet XwithoutA, Integer A) {

        input.receiveResult(new _FunctionalDependencyGroup(A, XwithoutA).buildDependency(input.relationName(), input.columnNames()));
    }

    
}








