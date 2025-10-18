/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package cz.cuni.mff.fdfinder.depminerservice.algorithm.service;

import cz.cuni.mff.fdfinder.depminerservice.algorithm.model._Input;
import cz.cuni.mff.fdfinder.depminerservice.algorithm.model._StrippedPartition;
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
 * @author pavel.koupil
 */
public class _StrippedPartitionGenerator {

	public static String nullValue = "null#" + Math.random();

	private JavaPairRDD<BitSet, _StrippedPartition> returnValue;

	//private Int2ObjectMap<Map<String, LongList>> translationMaps = new Int2ObjectOpenHashMap<>();

	public _StrippedPartitionGenerator() {
	}

	public JavaPairRDD<BitSet, _StrippedPartition> execute(_Input input) throws Exception {

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
                            return  new Tuple2<>(tuple._1._1, listPartitions);
                        })
                        .reduceByKey((x, y) -> {
                            x.addAll(y);
                            return x;
                        })
                        .mapToPair(tuple -> {
                            _StrippedPartition sp = new _StrippedPartition(tuple._1);
                            sp.addElements(tuple._2);
                            BitSet att = new BitSet();
                            att.set(tuple._1);
                            return new Tuple2<>(att, sp);
                        });
		

		// Načtení seznamů a vytvoření oddělených oddílů
		

		// úklid
		//this.translationMaps.clear();

		return this.returnValue;

	}


}
