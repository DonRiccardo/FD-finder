package de.metanome.algorithms.depminer.depminer_helper.modules;

import cz.cuni.mff.fdfinder.taneservice.algorithm.model._Input;
import de.metanome.algorithms.tane.model._StrippedPartitionSpark;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Class for generating {@link _StrippedPartitionSpark}
 */
public class _StrippedPartitionGenerator {

    private JavaPairRDD<Integer, _StrippedPartitionSpark> returnValue;

	public _StrippedPartitionGenerator() {
	}

    /**
     * Generate {@link _StrippedPartitionSpark} for specified {@code input}.
     * @param input {@link _Input} data object
     * @return {@link JavaPairRDD} of {@link Integer} attribute and its {@link _StrippedPartitionSpark}
     */
	public JavaPairRDD<Integer, _StrippedPartitionSpark> execute(_Input input) {

        this.returnValue = input.getRddData()
            // vytvorenie <<column ID, string hodnota>, [row IDs]>
            .flatMapToPair(tuple -> {
                List<Tuple2<Tuple2<Integer, String>, LongArrayList>> entities = new ArrayList<>();
                Row row = tuple._1();

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
        

}
