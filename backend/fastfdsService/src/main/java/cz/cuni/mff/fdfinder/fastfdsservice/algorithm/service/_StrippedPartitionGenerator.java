package cz.cuni.mff.fdfinder.fastfdsservice.algorithm.service;

import cz.cuni.mff.fdfinder.fastfdsservice.algorithm.model._Input;
import cz.cuni.mff.fdfinder.fastfdsservice.algorithm.model._StrippedPartitionSpark;
import cz.cuni.mff.fdfinder.fastfdsservice.algorithm.model._TupleEquivalenceClassRelation;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class _StrippedPartitionGenerator implements Serializable{

    // TODO: besser null Wert Ersatz?
    public static String nullValue = "null#" + Math.random();

    private JavaPairRDD<Integer, _StrippedPartitionSpark> returnValue;
    //private Int2ObjectMap<Map<String, IntList>> translationMaps = new Int2ObjectOpenHashMap<Map<String, IntList>>();
    private static Long2ObjectOpenHashMap<_TupleEquivalenceClassRelation> relationships;

    public _StrippedPartitionGenerator(Long2ObjectOpenHashMap<_TupleEquivalenceClassRelation> relationships) {

        this.relationships = relationships;

    }

    
    public JavaPairRDD<Integer, _StrippedPartitionSpark> execute(_Input input){

       // nacitani dat, vytvoreni stripped partitions
        this.returnValue = input.getRddData()
            // vytvorenie <<column ID, string hodnota>, [row IDs]>
            .flatMapToPair(tuple -> {
                List<Tuple2<Tuple2<Integer, String>, LongArrayList>> entities = new ArrayList<>();
                Row row = tuple._1();
                
                _TupleEquivalenceClassRelation relation = new _TupleEquivalenceClassRelation();
                this.relationships.put(tuple._2, relation);

                //System.out.println("PUT entry: "+tuple._2+" -> "+this.relationships.get(tuple._2));
                for (int i = 0; i < input.numberOfColumns(); i++) {
                    String value = row.getString(i);
                    //BitSet b = new BitSet();
                    //b.set(i);
                                 

                    LongArrayList l = new LongArrayList();  
                    l.add(tuple._2);
                    entities.add(new Tuple2<>(new Tuple2<>(i, value), l));

                }
                return entities.iterator();
            })
            .reduceByKey((x, y) -> {
                x.addAll(y);
                return x;
            })
            .filter(x -> x._2.size() > 1)
            .mapToPair(tuple -> {
                List<LongList> listPartitions = new ArrayList<>();
                listPartitions.add(tuple._2);
                //return  new Tuple2<>(tuple._1._1, listPartitions);
                return (new Tuple2<>(tuple._1._1, new Tuple2<>(listPartitions, tuple._2.size())));
            })
            .reduceByKey((x, y) -> {
                int elemCount = x._2 + y._2;
                x._1.addAll(y._1);
                return new Tuple2<>(x._1, elemCount);
            })
            .mapToPair(tuple -> {
                _StrippedPartitionSpark sp = new _StrippedPartitionSpark(tuple._2._1, tuple._2._2);

                BitSet att = new BitSet();
                att.set(tuple._1);
                //System.out.println("#entries = "+this.relationships.keySet().size());
                for (int partitionID = 0; partitionID < tuple._2._1.size(); partitionID++) {
                    
                    for (long rowID : tuple._2._1.get(partitionID)) {
                        //System.out.println("RELATION: "+rowID+" "+ relationships.get(rowID)+" add entry = "+tuple._1+"+"+partitionID);
                        this.relationships.get(rowID).addNewRelationship(tuple._1, partitionID);
                    }
                
                }                
                
                return new Tuple2<>(tuple._1, sp);
            });


        return this.returnValue;
        

    }

  
}