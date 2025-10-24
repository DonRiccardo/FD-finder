package cz.cuni.mff.fdfinder.fastfdsservice.algorithm.service;

import cz.cuni.mff.fdfinder.fastfdsservice.algorithm.model._DifferenceSet;
import cz.cuni.mff.fdfinder.fastfdsservice.algorithm.model._FunctionalDependency;
import cz.cuni.mff.fdfinder.fastfdsservice.algorithm.model._FunctionalDependencyGroup;
import cz.cuni.mff.fdfinder.fastfdsservice.algorithm.model._Input;
import it.unimi.dsi.fastutil.ints.Int2IntArrayMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

public class _FindCoversGenerator implements Serializable{

    private static _Input input;
    private String tableIdentifier;
    private int numberOfAttributes;
    private int maxLhs;

    public _FindCoversGenerator(_Input input, int maxLhs) {

        _FindCoversGenerator.input = input;
        this.maxLhs = maxLhs;
        this.numberOfAttributes = input.numberOfColumns();
    }
    
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
                    // mozne vyriesit aj inak
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
               /*
                System.out.println("FIND_GEN: ATT: "+ tuple._1+" Boolean: "+tuple._2._2);
                System.out.println("");
                for(_DifferenceSet d : tuple._2._1){
                    System.out.println(d.toString());
                }
                */
                
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
                
        /*
        for (int attribute = 0; attribute < numberOfAttributes; attribute++) {

            List<_DifferenceSet> tempDiffSet = new LinkedList<_DifferenceSet>();

            // Compute DifferenceSet modulo attribute (line 3 - Fig5 - FastFDs)
            for (_DifferenceSet ds : differenceSets) {
                BitSet obs = (BitSet) ds.getAttributes().clone();
                if (!obs.get(attribute)) {
                    continue;
                }
                obs.flip(attribute);
                tempDiffSet.add(new _DifferenceSet(obs));
            }

            // check new DifferenceSet (line 4 + 5 - Fig5 - FastFDs)
            if (tempDiffSet.size() == 0) {
                this.addFdToReceivers(new _FunctionalDependencyGroup(attribute, new IntArrayList()));
            } else if (this.checkNewSet(tempDiffSet)) {
                List<_DifferenceSet> copy = new LinkedList<_DifferenceSet>();
                copy.addAll(tempDiffSet);
                this.doRecusiveCrap(attribute, this.generateInitialOrdering(tempDiffSet), copy, new IntArrayList(), tempDiffSet,
                        result);
            }

        }
        */

        return result;

    }

    private boolean checkNewSet(List<_DifferenceSet> tempDiffSet) {

        for (_DifferenceSet ds : tempDiffSet) {
            if (ds.getAttributes().isEmpty()) {
                return false;
            }
        }

        return true;
    }

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
        // TODO: Comperator und TreeMap --> Tommy
        /*
        while (true) {

            if (counting.size() == 0) {
                break;
            }

            int biggestAttribute = -1;
            int numberOfOcc = 0;
            for (int attr : counting.keySet()) {

                if (biggestAttribute < 0) {
                    biggestAttribute = attr;
                    numberOfOcc = counting.get(attr);
                    continue;
                }

                int tempOcc = counting.get(attr);
                if (tempOcc > numberOfOcc) {
                    numberOfOcc = tempOcc;
                    biggestAttribute = attr;
                } else if (tempOcc == numberOfOcc) {
                    if (biggestAttribute > attr) {
                        biggestAttribute = attr;
                    }
                }
            }

            if (numberOfOcc == 0) {
                break;
            }

            result.add(biggestAttribute);
            counting.remove(biggestAttribute);
        }
        */
        return result;
    }

    private void doRecusiveCrap(int currentAttribute, IntList currentOrdering, List<_DifferenceSet> setsNotCovered,
                                IntList currentPath, List<_DifferenceSet> originalDiffSet, List<_FunctionalDependencyGroup> result)
            /*throws CouldNotReceiveResultException, ColumnNameMismatchException*/ {
        /*System.out.println("------ recursion ------");
        System.out.println("FIND_GEN: ordering: "+Arrays.toString(currentOrdering.toIntArray()));
        System.out.println("FIND_GEN: sets not covered: ");
        for(_DifferenceSet d : setsNotCovered) System.out.println(d.toString());
        System.out.println("");
        System.out.println("FIND_GEN: current Path: "+Arrays.toString(currentPath.toIntArray()));
        System.out.println("FIND_GEN: original DIFF set: ");
        for(_DifferenceSet d : originalDiffSet) System.out.println(d.toString());
        */
        // Basic Case
        // FIXME
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

        // TODO: Comperator und TreeMap --> Tommy
        /*
        while (true) {

            if (counting.size() == 0) {
                break;
            }

            int biggestAttribute = -1;
            int numberOfOcc = 0;
            for (int attr : counting.keySet()) {

                if (biggestAttribute < 0) {
                    biggestAttribute = attr;
                    numberOfOcc = counting.get(attr);
                    continue;
                }

                int tempOcc = counting.get(attr);
                if (tempOcc > numberOfOcc) {
                    numberOfOcc = tempOcc;
                    biggestAttribute = attr;
                } else if (tempOcc == numberOfOcc) {
                    if (biggestAttribute > attr) {
                        biggestAttribute = attr;
                    }
                }
            }

            if (numberOfOcc == 0) {
                break;
            }

            result.add(biggestAttribute);
            counting.remove(biggestAttribute);
        }
        */
        return result;
    }

    private List<_DifferenceSet> generateNextNotCovered(int attribute, List<_DifferenceSet> setsNotCovered) {

        List<_DifferenceSet> result = new LinkedList<_DifferenceSet>();

        for (_DifferenceSet ds : setsNotCovered) {

            if (!ds.getAttributes().get(attribute)) {
                result.add(ds);
            }
        }

        return result;
    }

    private void addFdToReceivers(_FunctionalDependencyGroup fdg) /*throws CouldNotReceiveResultException, ColumnNameMismatchException */{

        input.receiveResult((fdg.buildDependency(input.relationName(), input.columnNames())));

    }

    private boolean noOneCovers(List<BitSet> subSets, List<_DifferenceSet> originalDiffSet) {

        for (BitSet obs : subSets) {

            if (this.covers(obs, originalDiffSet)) {
                return false;
            }

        }

        return true;
    }

    private boolean covers(BitSet obs, List<_DifferenceSet> originalDiffSet) {

        for (_DifferenceSet diff : originalDiffSet) {

        	if (!obs.intersects(diff.getAttributes())) {
                return false;
            }
        }

        return true;
    }
    
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
