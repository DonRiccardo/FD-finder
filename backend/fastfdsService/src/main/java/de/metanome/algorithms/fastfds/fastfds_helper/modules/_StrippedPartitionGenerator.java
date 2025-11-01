package de.metanome.algorithms.fastfds.fastfds_helper.modules;

import cz.cuni.mff.fdfinder.fastfdsservice.algorithm.model._Input;
import de.metanome.algorithms.fastfds.fastfds_helper.modules.container._StrippedPartitionSpark;
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

/**
 * Generates stripped partitions {@link _StrippedPartitionSpark} from input data.
 */
public class _StrippedPartitionGenerator implements Serializable{

    // TODO: besser null Wert Ersatz?
    public static String nullValue = "null#" + Math.random();

    private JavaPairRDD<Integer, _StrippedPartitionSpark> returnValue;
    private static Long2ObjectOpenHashMap<_TupleEquivalenceClassRelation> relationships;

    public _StrippedPartitionGenerator(Long2ObjectOpenHashMap<_TupleEquivalenceClassRelation> relationships) {

        this.relationships = relationships;

    }

    /**
     * Executes the stripped partition generation pipeline.
     *
     * @param input {@link _Input} data object containing an {@link org.apache.spark.api.java.JavaRDD} of (Row, rowId) pairs and schema information
     * @return a {@link JavaPairRDD} where each key is a {@link BitSet} representing a single attribute,
     *         and each value is a {@link _StrippedPartitionSpark} containing equivalence classes for that attribute
     */
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
            // this will create stripped partitions
            .filter(x -> x._2.size() > 1)
            // create <CloumnID, <StrippedPartitions, number of IDs in partition>>
            .mapToPair(tuple -> {
                List<LongList> listPartitions = new ArrayList<>();
                listPartitions.add(tuple._2);
                return (new Tuple2<>(tuple._1._1, listPartitions));
            })
            // reduce by ColumnID -> merge partitions
            .reduceByKey((x, y) -> {
                x.addAll(y);
                return x;
            })
            // get <ColumnID, <StrippedPsrtitions, number of ID in all partitions>>
            // create SP object, add to relationships
            .mapToPair(tuple -> {
                _StrippedPartitionSpark sp = new _StrippedPartitionSpark(tuple._2);

                BitSet att = new BitSet();
                att.set(tuple._1);
                //System.out.println("#entries = "+this.relationships.keySet().size());
                for (int partitionID = 0; partitionID < tuple._2.size(); partitionID++) {
                    
                    for (long rowID : tuple._2.get(partitionID)) {
                        // add new relationship -> rowID is in SP with index partitionID for tuple._1 attribute
                        this.relationships.get(rowID).addNewRelationship(tuple._1, partitionID);
                    }
                
                }                
                
                return new Tuple2<>(tuple._1, sp);
            });


        return this.returnValue;
        

    }

  
}