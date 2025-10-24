/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package cz.cuni.mff.fdfinder.taneservice.algorithm.service;

import cz.cuni.mff.fdfinder.taneservice.algorithm.model._Input;
import cz.cuni.mff.fdfinder.taneservice.algorithm.model._StrippedPartitionSpark;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 *
 * @author Richard
 */
public class _StrippedPartitionGenerator {
    
        public static String nullValue = "null#" + Math.random();

	private JavaPairRDD<Integer, _StrippedPartitionSpark> returnValue;

	//private Int2ObjectMap<Map<String, LongList>> translationMaps = new Int2ObjectOpenHashMap<>();

	public _StrippedPartitionGenerator() {
	}

	public JavaPairRDD<Integer, _StrippedPartitionSpark> execute(_Input input) throws Exception {

		// nacitani dat, vytvoreni stripped partitions
                this.returnValue = input.getRddData()
                        // vytvorenie <<column ID, string hodnota>, [row IDs]>
                        .flatMapToPair(tuple -> {
                            List<Tuple2<Tuple2<Integer, String>, LongArrayList>> entities = new ArrayList<>();
                            Row row = tuple._1();

                            for (int i = 0; i < input.numberOfColumns(); i++) {
                                String value = row.getString(i);
                                //BitSet b = new BitSet();
                                //b.set(i);
                                
                                // ignorovanie NULL hodnot, vysledkom je, ze kazda bude ako jedinecna hodnota
                                if (value == null){
                                    continue;
                                }
                                
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
                            List<LongList> listPartitions = new ArrayList<LongList>();
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
                            return new Tuple2<>(tuple._1, sp);
                        });
		

		return this.returnValue;

	}
        
        /*
    private static JavaPairRDD<Integer, _StrippedPartitionSpark> computePLIS(JavaRDD<Tuple2<Row, Long>> dataWithIndex, int numColumns){
        
        JavaPairRDD<Integer, _StrippedPartitionSpark> plisData = dataWithIndex
            // create <<columnIndex, stringValue>, [rowId]> entities for each value in dataset
            .flatMapToPair(tuple -> {
                List<Tuple2<Tuple2<Integer, String>, LongBigArrayBigList>> entities = new ArrayList<>();
                Row row = tuple._1();
                  
                for (int i = 0; i < numColumns; i++) {
                    String value = row.getString(i);
                    //if (value != null){
                        LongBigArrayBigList l = new LongBigArrayBigList();
                        l.add(tuple._2);
                        entities.add(new Tuple2<>(new Tuple2<>(i, value), l));
                    //}
                }
                return entities.iterator();
            })
            // merge rowIds with same value in column
            .reduceByKey((x, y) -> {
                x.addAll(y);
                return x; 
            })
            // we need stripped partitions
            .filter(x -> x._2.size64() > 1)
            // we do not need the exact string value -> keep only columnIndex, rowIds (in list)
            .mapToPair(x -> {
                ObjectBigArrayBigList<LongBigArrayBigList> list = new ObjectBigArrayBigList<>();
                list.add(x._2);
                return (new Tuple2<>(x._1._1, new Tuple2<>(list, x._2.size64())));                
            })
            // PLIS in format <columnIndex, list of list(all rowIds with the same value)>
            .reduceByKey((x, y) -> {
                Long elemCount = x._2 + y._2;
                x._1.addAll(y._1);
                return new Tuple2<>(x._1, elemCount);
            })
            .mapValues(x -> {
                return new _StrippedPartitionSpark(x._1, x._2);
            });
        
        // PRINT Plis for checking or understanding
        /*
        plisData.foreach(data -> {
            System.out.print(data._1+" ATT, NUM elem: "+data._2.getElemCount()+", Error: "+data._2.getError());
            for (int i=0; i< data._2.getStrippedPartition().size64(); i++){
                System.out.print("{");
                for (Object o : data._2.getStrippedPartition().get(i)) {
                    System.out.print(o+", ");
                }
                System.out.print("}, ");
            }
            System.out.println();
        });
        
        return plisData;
    }
*/
}
