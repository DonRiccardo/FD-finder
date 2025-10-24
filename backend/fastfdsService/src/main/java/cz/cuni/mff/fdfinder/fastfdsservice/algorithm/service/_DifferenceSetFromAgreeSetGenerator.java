/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package cz.cuni.mff.fdfinder.fastfdsservice.algorithm.service;

//import de.metanome.algorithms.fastfds_spark.model._AgreeSet;

import cz.cuni.mff.fdfinder.fastfdsservice.algorithm.model._DifferenceSet;
import cz.cuni.mff.fdfinder.fastfdsservice.algorithm.model._StrippedPartitionSpark;
import cz.cuni.mff.fdfinder.fastfdsservice.algorithm.model._TupleEquivalenceClassRelation;
import it.unimi.dsi.fastutil.longs.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 *
 * @author Richard
 */
public class _DifferenceSetFromAgreeSetGenerator implements Serializable{
    
    private Long2ObjectOpenHashMap<_TupleEquivalenceClassRelation> relationships;
    private final int numOfAttributes;
    
    public _DifferenceSetFromAgreeSetGenerator (Long2ObjectOpenHashMap<_TupleEquivalenceClassRelation> relationships, int numOfAttributes){
        
        this.relationships = relationships;
        this.numOfAttributes = numOfAttributes;
        //System.out.println("DIFF_GEN: "+relationships.size()+" size relationships");
    }
    
    private static class ListComparatorBottleneck implements Comparator<LongList> {

        @Override
        public int compare(LongList x, LongList y) {
            
            if (x.size() - y.size() != 0) {
                    return y.size() - x.size();
            }
            for (int i = 0; i < x.size(); i++) {
                if (x.getLong(i) == y.getLong(i)) {
                    continue;
                }
                return (int) (y.getLong(i) - x.getLong(i));
            }
            return 0;
        }

    }
    
    public JavaRDD<_DifferenceSet> executeBottleneck(JavaPairRDD<Integer, _StrippedPartitionSpark> partitions){
        
        //System.out.println("DIFF_GEN: "+partitions.count()+" SP count");
        
        JavaRDD<_DifferenceSet> returnValue =  partitions
            .flatMapToPair(tuple -> {
                List<Tuple2<Boolean, List<LongList>>> result = new ArrayList<>();                        

                for (LongList listValues : tuple._2.getValues()){
                    LinkedList<LongList> partitionsIDs = new LinkedList<>();
                    listValues.sort(null); //natural ordering sort IDs of tuples
                    
                    //System.out.println("DIFF_GEN: "+listValues+" some equivalence class SP");

                    partitionsIDs.add(listValues);

                    result.add(new Tuple2<>(true, partitionsIDs));
                }
                return result.iterator();
            })
            .reduceByKey((x, y) -> {
                if (x.size() < y.size()){
                    y.addAll(x);
                    return y;
                }

                x.addAll(y);
                return x;
            })
            // vypocet MAX mnozin ako bottleneck!
            .flatMap(tuple -> {
                
                TreeSet<LongList> treePartitions = new TreeSet<>(new ListComparatorBottleneck());
                Long2ObjectMap<LongList> indexOfPartitionWithLong = new Long2ObjectOpenHashMap<>();
                Set<LongList> maxSets = new HashSet<>();
                
                for (LongList IDs : tuple._2){
                    treePartitions.add(IDs);
                }                     
                
                Iterator<LongList> treeIterator = treePartitions.iterator();
                
                long currentIndex = 0;
                LongList currentList;

                while (treeIterator.hasNext()) {
                    currentList = treeIterator.next();
                    
                    //System.out.println("DIFF_GEN: "+currentList.toArray()+" partition");
                    
                    if (!this.isSubset(currentList, indexOfPartitionWithLong)) {
                        maxSets.add(currentList);
                        
                        //System.out.println("DIFF_GEN: "+currentList+" added to MAX sets");
                        
                        for (long e : currentList) {
                            if (!indexOfPartitionWithLong.containsKey(e)) {
                                indexOfPartitionWithLong.put(e, new LongArrayList());
                            }
                            indexOfPartitionWithLong.get(e).add(currentIndex);
                        }
                    }

                    currentIndex++;
                }

                indexOfPartitionWithLong.clear();
                treePartitions.clear();                
                
                //System.out.println("DIFF_GEN: "+maxSets.size()+ " MAX sets size");
                
                return maxSets.iterator();
            })
            // rovno sa vytvaraju _DifferenceSets, usetri sa praca s vytvaranim _AgreeSets
            .flatMapToPair(maxSet -> {
                
                //System.out.println("DIFF_GEN: "+maxSet+" MAX set");
                
                List<Tuple2<_DifferenceSet, Boolean>> diffSets = new LinkedList<>();
                
                for (int i = 0; i < maxSet.size()-1; i++) {
                    _TupleEquivalenceClassRelation firstRelationship = this.relationships.get(maxSet.getLong(i));
                    for (int j = i+1; j < maxSet.size(); j++) {
                        
                        BitSet diff = new BitSet();
                        diff.set(0, this.numOfAttributes);
                        _TupleEquivalenceClassRelation secondRelationship = this.relationships.get(maxSet.getLong(j));
                        
                        //System.out.println("DIFF_GEN: MAX entries: "+firstRelationship+" x "+secondRelationship);
                        
                        BitSet agreeBits = firstRelationship.intersectWithAndComputeAgreeBitSet(secondRelationship);
                        diff.andNot(agreeBits);
                        diffSets.add(new Tuple2<> (new _DifferenceSet(diff), true));
                        
                        //System.out.println("DIFF_GEN: MAX agree/diff: "+ agreeBits+" / "+diff);
                        
                    }
                    
                }
                
                // append required _DifferenceSer with all attributes set 
                // this correspond to an empty _AgreeSet
                BitSet diff = new BitSet();
                diff.set(0, this.numOfAttributes);
                diffSets.add(new Tuple2<> (new _DifferenceSet(diff), true));
                
                return diffSets.iterator();
            })
            .reduceByKey((x,y) -> {return x;})
            .map(x -> {return x._1;});
        
        return returnValue;
}

    private boolean isSubset(LongList currentList, Map<Long, LongList> indexOfPartitionWithLong) {

        boolean first = true;
        LongList positions = new LongArrayList();
        for (long e : currentList) {
            if (!indexOfPartitionWithLong.containsKey(e)) {
                return false;
            }
            if (first) {
                positions.addAll(indexOfPartitionWithLong.get(e));
                first = false;
            } else {

                this.intersect(positions, indexOfPartitionWithLong.get(e));
                // FIXME: Throws UnsupportedOperationExeption within fastUtil
                // positions.retainAll(index.get(e));
            }
            if (positions.isEmpty()) {
                return false;
            }
        }
        return true;
    }
    
    private void intersect(LongList positions, LongList indexSet) {

        LongSet toRemove = new LongArraySet();
        for (long l : positions) {
            if (!indexSet.contains(l)) {
                toRemove.add(l);
            }
        }
        positions.removeAll(toRemove);
    }
    
}
