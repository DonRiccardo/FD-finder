package de.metanome.algorithms.fastfds.modules;

import de.metanome.algorithms.fastfds.modules.container._DifferenceSet;
import de.metanome.algorithms.fastfds.fastfds_helper.modules.container._FunctionalDependencyGroup;
import cz.cuni.mff.fdfinder.fastfdsservice.algorithm.model._Input;
import it.unimi.dsi.fastutil.ints.Int2IntArrayMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Generator class for discovering minimal functional dependencies (FDs) from
 * a set of {@link _DifferenceSet difference sets}. It recursively computes the minimal
 * left-hand sides (LHS) for each attribute that is functionally determined.
 */
public class _FindCoversGenerator implements Serializable{

    private static _Input input;
    private String tableIdentifier;
    private int numberOfAttributes;
    private int maxLhs;

    /**
     * Constructs a new {@link _FindCoversGenerator}.
     *
     * @param input {@link _Input} interface to receive discovered functional dependencies.
     * @param maxLhs Maximum allowed size for LHS of a functional dependency.
     */
    public _FindCoversGenerator(_Input input, int maxLhs) {

        _FindCoversGenerator.input = input;
        this.maxLhs = maxLhs;
        this.numberOfAttributes = input.numberOfColumns();
    }

    /**
     * Comparator for ordering attributes based on frequency of occurrence
     * in difference sets and attribute index.
     */
    private static class OrderingComparator implements Comparator<Tuple2<Integer, Integer>> {
        // tuple as <ID of attribute, # of occurencies>
        // first order by # of occurencies
        // second by smaller ID of attribute
        @Override
        public int compare(Tuple2<Integer, Integer> x, Tuple2<Integer, Integer> y) {
           
            if (Objects.equals(x._2, y._2)) {
                return x._1 - y._1;
            }
            else{
                return y._2 - x._2;
            }
            
        }

    }

    /**
     * Executes functional dependency discovery from a given RDD of {@link _DifferenceSet difference sets}.
     *
     * @param differenceSets {@link JavaRDD} of {@link _DifferenceSet} representing differences between tuples.
     * @return List of {@link _FunctionalDependencyGroup} representing discovered minimal FDs.
     */
    public List<_FunctionalDependencyGroup> execute(JavaRDD<_DifferenceSet> differenceSets) {

        List<_FunctionalDependencyGroup> result = new LinkedList<>();
        
        differenceSets
            // generate entries <attribute A, <DIFF without A, is DIFF NOT empty>>
            .flatMapToPair(diff -> {
                List<Tuple2<Integer, Tuple2<List<_DifferenceSet>, Boolean>>> entries = new LinkedList<>();
                                
                for (int i = 0; i < this.numberOfAttributes; i++) {
                    
                    if (diff.getAttributes().get(i)){
                        List<_DifferenceSet> diffList = new LinkedList<>();                        
                        
                        BitSet diffAttributes = (BitSet) diff.getAttributes().clone();
                        diffAttributes.clear(i);
                        
                        _DifferenceSet diffWithoutA = new _DifferenceSet(diffAttributes);
                        diffList.add(diffWithoutA);
                       // System.out.println("FIND_GEN: ATT: "+i+" DIFF/A: "+diffWithoutA);
                        entries.add(new Tuple2<>(i, new Tuple2<>(diffList, !diff.getAttributes().isEmpty())));
                        
                    }  
                    // vytvaranie prazdnych listov pre dany atribut A
                    // ak po redukovani, bude nebude mat atribut A ziadnu DIFF set => FD: EMPTY_SET->A
                    else{
                        entries.add(new Tuple2<>(i, new Tuple2<>(new LinkedList<_DifferenceSet>(), true)));
                    }
                }
                
                return entries.iterator();
            })
            .reduceByKey((x, y) -> {
                List<_DifferenceSet> diff = new LinkedList<>();
                diff.addAll(x._1);
                diff.addAll(y._1);
                return new Tuple2<>(diff, x._2 && y._2);
            })
            // filter only TRUE values -> list does NOT contain empty DIFF set
            .filter(x -> x._2._2)
            .foreach(tuple -> {
                
                if (tuple._2._1.isEmpty()) {
                   // System.out.println("FIND_GEN: emptySet -> A");
                    this.addFdToReceivers(new _FunctionalDependencyGroup(tuple._1, new IntArrayList()));
                } else {
                    List<_DifferenceSet> copy = new LinkedList<>();
                    copy.addAll(tuple._2._1);
                    this.doRecusiveCrap(tuple._1, this.generateInitialOrdering(tuple._2._1), copy, new IntArrayList(), tuple._2._1,
                            result);
                   // System.out.println("FIND_GEN: did recursive crap");
                    //System.out.println("---------------------------------------------------------------------");
                }
            });

        return result;

    }

    /**
     * Generates an initial attribute ordering based on frequency in the remaining difference sets.
     *
     * @param tempDiffSet List of {@link _DifferenceSet}.
     * @return {@link IntList} containing attribute IDs in order of descending occurrence frequency.
     */
    private IntList generateInitialOrdering(List<_DifferenceSet> tempDiffSet) {

        IntList result = new IntArrayList();

        Int2IntMap counting = new Int2IntArrayMap();
        for (_DifferenceSet ds : tempDiffSet) {

            int lastIndex = ds.getAttributes().nextSetBit(0);

            while (lastIndex != -1) {
                if (!counting.containsKey(lastIndex)) {
                    counting.put(lastIndex, 1);
                } else {
                    counting.put(lastIndex, counting.get(lastIndex) + 1);
                }
                lastIndex = ds.getAttributes().nextSetBit(lastIndex + 1);
            }
        }
        
        TreeSet<Tuple2<Integer, Integer>> ordering = new TreeSet<>(new OrderingComparator());
        
        for (int key : counting.keySet()){
            ordering.add(new Tuple2<>(key, counting.get(key)));
        }
        
        if (!ordering.isEmpty() && ordering.first()._2 == 0) return result;
        
        for (Tuple2<Integer, Integer> att : ordering) {
            result.add(att._1);
        }

        return result;
    }

    /**
     * Recursive method to discover minimal LHS for the FD {@code currentPath -> currentAttribute}.
     *
     * @param currentAttribute Attribute ID on RHS.
     * @param currentOrdering {@link IntList} Attribute ordering for recursion.
     * @param setsNotCovered {@link List} Remaining difference sets not yet covered.
     * @param currentPath {@link IntList} Current LHS path being constructed.
     * @param originalDiffSet {@link List} Original difference sets for minimality check.
     * @param result {@link List} to collect discovered {@link _FunctionalDependencyGroup}.
     */
    private void doRecusiveCrap(int currentAttribute, IntList currentOrdering, List<_DifferenceSet> setsNotCovered,
                                IntList currentPath, List<_DifferenceSet> originalDiffSet, List<_FunctionalDependencyGroup> result) {

        if (!currentOrdering.isEmpty() && /* BUT */setsNotCovered.isEmpty()) {
            //if (this.debugSysout)
              //  System.out.println("no FDs here");
            //System.out.println("FIND_GEN: recursive crap -> IF A");
            return;
        }

        if (setsNotCovered.isEmpty()) {
            //System.out.println("FIND_GEN: recursive crap -> IF B");
            List<BitSet> subSets = this.generateSubSets(currentPath);
            if (this.noOneCovers(subSets, originalDiffSet)) {
                _FunctionalDependencyGroup fdg = new _FunctionalDependencyGroup(currentAttribute, currentPath);
                this.addFdToReceivers(fdg);
                result.add(fdg);
                //System.out.println("FIND_GEN: recursive crap -> IF C");
            } else {
                /*if (this.debugSysout) {
                    System.out.println("FD not minimal");
                    System.out.println(new _FunctionalDependencyGroup(currentAttribute, currentPath));
                }*/
               // System.out.println("FIND_GEN: recursive crap -> ELSE A");
            }

            return;
        }

        if (currentPath.size() >= maxLhs) {
            return;
        }

        // Recusive Case
        for (int i = 0; i < currentOrdering.size(); i++) {
           // System.out.println("FIND_GEN: recursive crap -> FOR loop");
            List<_DifferenceSet> next = this.generateNextNotCovered(currentOrdering.getInt(i), setsNotCovered);
            IntList nextOrdering = this.generateNextOrdering(next, currentOrdering, currentOrdering.getInt(i));
            IntList currentPathCopy = new IntArrayList(currentPath);
            currentPathCopy.add(currentOrdering.getInt(i));
            this.doRecusiveCrap(currentAttribute, nextOrdering, next, currentPathCopy, originalDiffSet, result);
        }

    }

    /**
     * Generates the next attribute ordering for recursion after selecting an attribute.
     *
     * <p>The ordering is based on the frequency of attributes in the remaining difference sets,
     * preserving descending frequency order and attribute index as tie-breaker.</p>
     *
     * @param next Remaining difference sets after removing the selected attribute.
     * @param currentOrdering Current ordering of attributes.
     * @param attribute Selected attribute that was just added to the LHS.
     * @return {@link IntList} of attribute IDs in new ordering for recursive calls.
     */
    private IntList generateNextOrdering(List<_DifferenceSet> next, IntList currentOrdering, int attribute) {

        IntList result = new IntArrayList();

        Int2IntMap counting = new Int2IntArrayMap();
        boolean seen = false;
        for (int i = 0; i < currentOrdering.size(); i++) {

            if (!seen) {
                if (currentOrdering.getInt(i) == attribute) {
                    seen = true;
                }
            } else {

                counting.put(currentOrdering.getInt(i), 0);
                for (_DifferenceSet ds : next) {

                    if (ds.getAttributes().get(currentOrdering.getInt(i))) {
                        counting.put(currentOrdering.getInt(i), counting.get(currentOrdering.getInt(i)) + 1);
                    }
                }
            }
        }
        
        TreeSet<Tuple2<Integer, Integer>> ordering = new TreeSet<>(new OrderingComparator());
        
        for (int key : counting.keySet()){
            ordering.add(new Tuple2<>(key, counting.get(key)));
        }
        
        if (!ordering.isEmpty() && ordering.first()._2 == 0) return result;
        
        for (Tuple2<Integer, Integer> att : ordering) {            
            result.add(att._1);
        }

        return result;
    }

    /**
     * Generates the next list of difference sets not covered by a given attribute.
     *
     * @param attribute Attribute ID to check against.
     * @param setsNotCovered Current list of {@link _DifferenceSet} not covered.
     * @return {@link List} of {@link _DifferenceSet} where the attribute is not present.
     */
    private List<_DifferenceSet> generateNextNotCovered(int attribute, List<_DifferenceSet> setsNotCovered) {

        List<_DifferenceSet> result = new LinkedList<_DifferenceSet>();

        for (_DifferenceSet ds : setsNotCovered) {

            if (!ds.getAttributes().get(attribute)) {
                result.add(ds);
            }
        }

        return result;
    }

    /**
     * Sends a discovered {@link _FunctionalDependencyGroup} to the {@link _Input} receiver.
     *
     * @param fdg {@link _FunctionalDependencyGroup} representing a discovered minimal FD.
     */
    private void addFdToReceivers(_FunctionalDependencyGroup fdg) /*throws CouldNotReceiveResultException, ColumnNameMismatchException */{

        input.receiveResult((fdg.buildDependency(input.relationName(), input.columnNames())));

    }

    /**
     * Checks if none of the given candidate subsets is covered by the original difference sets.
     *
     * @param subSets List of {@link BitSet} candidate LHS subsets.
     * @param originalDiffSet Original list of {@link _DifferenceSet}.
     * @return {@code true} if no subset is covered (i.e., it forms a minimal FD), {@code false} otherwise.
     */
    private boolean noOneCovers(List<BitSet> subSets, List<_DifferenceSet> originalDiffSet) {

        for (BitSet obs : subSets) {

            if (this.covers(obs, originalDiffSet)) {
                return false;
            }

        }

        return true;
    }

    /**
     * Checks if a given candidate LHS (BitSet) covers all original difference sets.
     *
     * @param obs Candidate LHS as {@link BitSet}.
     * @param originalDiffSet Original list of {@link _DifferenceSet}.
     * @return {@code true} if {@code obs} covers all difference sets, {@code false} otherwise.
     */
    private boolean covers(BitSet obs, List<_DifferenceSet> originalDiffSet) {

        for (_DifferenceSet diff : originalDiffSet) {

        	if (!obs.intersects(diff.getAttributes())) {
                return false;
            }
        }

        return true;
    }

    /**
     * Generates all subsets of the given LHS path by removing one attribute at a time.
     *
     * @param currentPath {@link IntList} representing current LHS path.
     * @return {@link List} of {@link BitSet} representing all proper subsets of {@code currentPath}.
     */
    private List<BitSet> generateSubSets(IntList currentPath) {

        List<BitSet> result = new LinkedList<BitSet>();

        BitSet obs = new BitSet();
        for (int i : currentPath) {
            obs.set(i);
        }

        for (int i : currentPath) {

            BitSet obs_ = (BitSet) obs.clone();
            obs_.flip(i);
            result.add(obs_);

        }

        return result;
    }
}
