/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package cz.cuni.mff.fdfinder.taneservice.algorithm;

import cz.cuni.mff.fdfinder.taneservice.algorithm.model.*;
import cz.cuni.mff.fdfinder.taneservice.algorithm.service._StrippedPartitionGenerator;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.*;

/**
 * Class representing actual working algorithm.
 * @author Richard
 */
public class TaneSparkAlgorithm implements Serializable{
    
    private static  _Input input;
    private static JavaSparkContext context;
    private final int maxLhs;
    
    public TaneSparkAlgorithm(_Input input, int maxLhs, JavaSparkContext context){
        this.input = input;
        if (maxLhs < 0) {

            this.maxLhs = input.numberOfColumns();
        }
        else {

            this.maxLhs = Math.min(maxLhs, input.numberOfColumns());
        }
        this.context = context;
    }
    

    private int numberAttributes;
    private long numberTuples;
        
    private JavaPairRDD<Integer, _StrippedPartitionSpark> plis = null;
    private Map<BitSet, CombinationHelperSpark> level0 = null;
    private JavaPairRDD<BitSet, CombinationHelperSpark> level1 = null;
    private JavaPairRDD<BitSet, List<BitSet>> prefix_blocks = null;

    /**
     * Run algorithm
     */
    public void execute() {
        
        loadData();
        
        // Initialize Level 0
        CombinationHelperSpark chLevel0 = new CombinationHelperSpark();
        BitSet rhsCandidatesLevel0 = new BitSet();
        rhsCandidatesLevel0.set(0, numberAttributes);
        chLevel0.setRhsCandidates(rhsCandidatesLevel0);
        _StrippedPartitionSpark spLevel0 = new _StrippedPartitionSpark(numberTuples);
        chLevel0.setPartition(spLevel0);
        spLevel0 = null;
        level0 = new HashMap<>();
        level0.put(new BitSet(), chLevel0);
        
        // Initialize Level 1        
        ArrayList<Tuple2<BitSet, CombinationHelperSpark>> listLVL1 = new ArrayList<>();
        
        for (int i = 0; i < numberAttributes; i++) {
            BitSet combinationLevel1 = new BitSet();
            combinationLevel1.set(i);

            CombinationHelperSpark chLevel1 = new CombinationHelperSpark();
            BitSet rhsCandidatesLevel1 = new BitSet();
            rhsCandidatesLevel1.set(0, numberAttributes);
            chLevel1.setRhsCandidates(rhsCandidatesLevel1);
            
            _StrippedPartitionSpark spLevel1;
            List<_StrippedPartitionSpark> lsp = plis.lookup(i);
            if (lsp.size() == 1){
                spLevel1 = lsp.get(0);
            }
            else{
                spLevel1 = new _StrippedPartitionSpark(numberTuples);
            }
             
            chLevel1.setPartition(spLevel1);
            listLVL1.add(new Tuple2<>(combinationLevel1, chLevel1));            
        }
        
        
        level1 = context.parallelizePairs(listLVL1);
        listLVL1 = null;

        int l = 0;
        while (level1.count()>0 && l < numberAttributes) {
            // compute dependencies for a level
            System.out.println("\n START LVL: "+l +"\n");
            computeDependencies();

            // prune the search space
            prune();

            // compute the combinations for the next level
            if (l >= maxLhs){
                break;
            }
            generateNextLevel();
            l++;
        }
    }
    
    // vytvorenie Stripped Partitions

    /**
     * Load data and create {@link _StrippedPartitionSpark}
     * @throws Exception
     */
    private void loadData() {
       
        this.numberAttributes = input.numberOfColumns();
        this.numberTuples = input.numberOfRows();
       
        _StrippedPartitionGenerator spGen = new _StrippedPartitionGenerator();
        plis = spGen.execute(this.input);
        System.out.println("SP DONE...");
    }

    /**
     *
     */
    private void computeDependencies() {
        
        level1 = level1
                .mapToPair(tuple -> {
                    if(!tuple._2.isValid()){
                        return tuple;
                    }
                    
                    BitSet intersection = (BitSet) tuple._1.clone();
                    intersection.and(tuple._2.getRhsCandidates());
                    
                    BitSet Xclone = (BitSet) tuple._1.clone();
                                        
                    for (int A = intersection.nextSetBit(0); A >= 0; A = intersection.nextSetBit(A + 1)){
                        Xclone.clear(A);
                        //System.out.println("Xclone: "+Xclone+" LVL0 = "+level0.get(Xclone));
                        if(!level0.get(Xclone).isValid()){
                            Xclone.set(A);
                            continue;
                        }
                        
                        _StrippedPartitionSpark spX = tuple._2.getPartition();
                        _StrippedPartitionSpark spXwithoutA = level0.get(Xclone).getPartition();
                        
                        if(spX.getError() == spXwithoutA.getError()){
                            
                            processFunctionalDependency((BitSet)Xclone.clone(), A);
                           
                            // remove A from C_plus(X)    
                            BitSet newRhsCandidates = (BitSet) tuple._2.getRhsCandidates().clone();
                            newRhsCandidates.clear(A);

                            // remove all B in R\X from C_plus(X)
                            BitSet RwithoutX = new BitSet();
                            // set to R
                            RwithoutX.set(0, numberAttributes);
                            // remove X
                            RwithoutX.andNot(tuple._1);

                            for (int i = RwithoutX.nextSetBit(0); i >= 0; i = RwithoutX.nextSetBit(i + 1)) {
                                newRhsCandidates.clear(i);
                            }
                            
                            tuple._2.setRhsCandidates(newRhsCandidates);
                            
                        }
                        
                        Xclone.set(A);
                    }
                    
                    return tuple;
                });

    }

    /**
     *
     */
    private void prune(){
        
        Map<BitSet, CombinationHelperSpark> level1asMap = level1.collectAsMap();
        
        level1 = level1
                .mapToPair(tuple -> {
                      
                    if(!tuple._2.isValid() || tuple._2.getPartition().getError()!=0){
                        
                        return tuple;
                    }
                    
                    BitSet rhsXwithoutX = (BitSet) tuple._2.getRhsCandidates().clone();
                    rhsXwithoutX.andNot(tuple._1);
                    BitSet xUnionAWithoutB = (BitSet) tuple._1.clone();
                    
                    for (int A = rhsXwithoutX.nextSetBit(0); A >= 0; A = rhsXwithoutX.nextSetBit(A + 1)){
                                                
                        BitSet intersect = new BitSet();
                        intersect.set(0, numberAttributes);
                        
                        xUnionAWithoutB.set(A);

                        for (int B = tuple._1.nextSetBit(0); B >= 0; B = tuple._1.nextSetBit(B + 1)){
                            xUnionAWithoutB.clear(B);
                            intersect.and(level1asMap.get(xUnionAWithoutB).getRhsCandidates());
                            xUnionAWithoutB.set(B);
                        }
                        
                        if(intersect.get(A)){

                            BitSet lhs = (BitSet) tuple._1.clone();
                            processFunctionalDependency(lhs, A);
                            
                            BitSet newRhs = (BitSet) tuple._2.getRhsCandidates().clone();
                            newRhs.clear(A);
                            tuple._2.setRhsCandidates(newRhs);
                            tuple._2.setInvalid();
                        }

                        xUnionAWithoutB.clear(A);
                    }
                    
                    
                    return tuple;
                })
                .reduceByKey((x,y) -> x);

    }

    /**
     *
     * @param pt1
     * @param pt2
     * @return
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
        // iterate over second stripped partition.
        for (int i = 0; i < pt2List.size(); i++) {
            for (long t_id : pt2List.get(i)) {
                // tuple is also in an equivalence class of pt1
                if (tTable.getLong(t_id) != -1) {
                    partition.get((int)tTable.getLong(t_id)).add(t_id);
                }
            }
            for (long tId : pt2List.get(i)) {
                // if condition not in the paper;
                if (tTable.getLong(tId) != -1) {
                    if (partition.get((int)tTable.getLong(tId)).size() > 1) {
                        LongList eqClass = partition.get((int)tTable.getLong(tId));
                        result.add(eqClass);
                        noOfElements += eqClass.size();
                    }
                    partition.set((int)tTable.getLong(tId), new LongArrayList());
                }
            }
        }

        return new _StrippedPartitionSpark(result, noOfElements);
    }

    /**
     * Generates new level for finding FDs also initialize C+ for new level.
     */
    private void generateNextLevel(){
        // LVL1 collectujem aj v PRUNE, aby som s tym mohol pracovat. Pouziva sa na overenie validity
        // narocne, ale nie je mozne pracovat s vnorenymi RDD
        level0 = level1.collectAsMap();

        level1 = null;
        buildPrefixBlocks();
        level1 = prefix_blocks
                .filter(x -> x._2.size() >= 2)
                .flatMapToPair(tuple -> {
                    List<Tuple2<BitSet, BitSet>> combinations = getListCombinations(tuple._2);
                    return combinations.iterator();
                })
                .map(tuple -> {
                    BitSet X = (BitSet) tuple._1.clone();
                    X.or(tuple._2);
                    return new Tuple3<>(tuple._1, tuple._2, X);
                })
                .filter(x -> checkSubsets(x._3()))
                .mapToPair(tuple -> {
                    _StrippedPartitionSpark st = null;
                    CombinationHelperSpark ch = new CombinationHelperSpark();
                    
                    if (level0.get(tuple._1()).isValid() && level0.get(tuple._2()).isValid()) {

                        st = multiply(level0.get(tuple._1()).getPartition(), level0.get(tuple._2()).getPartition());
                    } else {

                        ch.setInvalid();
                    }
                    BitSet rhsCandidates = new BitSet();

                    ch.setPartition(st);
                    ch.setRhsCandidates(rhsCandidates);
                    
                    return new Tuple2<>(tuple._3(), ch);
                    
                })
                
                // INITIALIZECPLUSFORLEVEL
                // skombinovanie funkcii, pretoze pred prvym spustenim levlu to uz nastavene je
                // dalsie levly sa pocitaju v generovani, tak rovno nastavit aj Cplus
                .flatMapToPair(tuple -> {
                    List<Tuple2<Tuple2<BitSet, CombinationHelperSpark>, BitSet>> rhsCwithoutA = new ArrayList<>();

                    BitSet Xclone = (BitSet) tuple._1.clone();
                    for (int A = tuple._1.nextSetBit(0); A >= 0; A = tuple._1.nextSetBit(A + 1)) {
                        Xclone.clear(A);
                        BitSet CxwithoutA = level0.get(Xclone).getRhsCandidates();
                        rhsCwithoutA.add(new Tuple2<>(tuple, CxwithoutA));
                        Xclone.set(A);
                    }

                    if (rhsCwithoutA.isEmpty()){
                        rhsCwithoutA.add(new Tuple2<>(tuple, new BitSet()));
                    }

                    return rhsCwithoutA.iterator();
                })
                .reduceByKey((x, y) -> {
                    BitSet newRhs = (BitSet) x.clone();
                    newRhs.and(y);
                    return newRhs;
                })
                .mapToPair(c -> {
                    c._1._2.setRhsCandidates(c._2);
                    return c._1;
                });

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
     * Get prefix of BitSet by copying it and removing the last Bit.
     *
     * @param bitset
     * @return A new BitSet, where the last set Bit is cleared.
     */
    private BitSet getPrefix(BitSet bitset) {
        BitSet prefix = (BitSet) bitset.clone();
        prefix.clear(getLastSetBitIndex(prefix));
        return prefix;
    }
    
    /**
     * Build the prefix blocks for a level. It is a HashMap containing the
     * prefix as a key and the corresponding attributes as  the value.
     */
    private void buildPrefixBlocks() {        
        List<Tuple2<BitSet, BitSet>> listPrefixBlocks = new ArrayList<>();
        
        for (BitSet level_iter : level0.keySet()) {
            BitSet prefix = getPrefix(level_iter);
            listPrefixBlocks.add(new Tuple2<>(prefix, level_iter));            
        }
        
        prefix_blocks = context.parallelizePairs(listPrefixBlocks)
                .groupByKey()
                .mapToPair(tuple -> {
                    List<BitSet> list = new ArrayList<>();
    
                    for (BitSet bitset : tuple._2) {
                        list.add(bitset);
                    }
                    return new Tuple2<>(tuple._1, list);
                });
    }
    
    
    /**
     * Get all combinations, which can be built out of the elements of a prefix block
     *
     * @param list: List of BitSets, which are in the same prefix block.
     * @return All combinations of the BitSets.
     */
    private List<Tuple2<BitSet, BitSet>> getListCombinations(List<BitSet> list) {
        List<Tuple2<BitSet, BitSet>> combinations = new ObjectArrayList<>();
        for (int a = 0; a < list.size(); a++) {
            for (int b = a + 1; b < list.size(); b++) {
                combinations.add(new Tuple2<>(list.get(a), list.get(b)));
                //System.out.println("COMBINATIONS: "+list.get(a)+" & "+list.get(b));
            }
        }
        return combinations;
    }
    
    /**
     * Checks whether all subsets of X (with length of X - 1) are part of the last level.
     * Only if this check return true X is added to the new level.
     *
     * @param X
     * @return
     */
    private boolean checkSubsets(BitSet X) {
        boolean xIsValid = true;

        // clone of X for usage in the following loop
        BitSet Xclone = (BitSet) X.clone();

        for (int l = X.nextSetBit(0); l >= 0; l = X.nextSetBit(l + 1)) {
            Xclone.clear(l);
            if (!level0.containsKey(Xclone)) {
                xIsValid = false;
                break;
            }
            Xclone.set(l);
        }

        return xIsValid;
    }

    /**
     * Processing founf FD, adding it into a result.
     * @param XwithoutA LHS of a FD
     * @param A RHS of a FD
     */
    private void processFunctionalDependency(BitSet XwithoutA, Integer A) {

        this.input.receiveResult(new _FunctionalDependencyGroup(A, XwithoutA).buildDependency(this.input.relationName(), this.input.columnNames()));
    }

    
}








