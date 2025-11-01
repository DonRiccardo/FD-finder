package de.metanome.algorithms.depminer.depminer_helper.modules;

import cz.cuni.mff.fdfinder.depminerservice.algorithm.model._Input;
import de.metanome.algorithms.depminer.depminer_helper.modules.container._StrippedPartition;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Generates stripped partitions {@link _StrippedPartition} from input data.
 */
public class _StrippedPartitionGenerator {

	private JavaPairRDD<BitSet, _StrippedPartition> returnValue;

	public _StrippedPartitionGenerator() {
	}

    /**
     * Executes the stripped partition generation pipeline.
     *
     * @param input {@link _Input} data object containing an {@link org.apache.spark.api.java.JavaRDD} of (Row, rowId) pairs and schema information
     * @return a {@link JavaPairRDD} where each key is a {@link BitSet} representing a single attribute,
     *         and each value is a {@link _StrippedPartition} containing equivalence classes for that attribute
     */
	public JavaPairRDD<BitSet, _StrippedPartition> execute(_Input input) {

		// nacitani dat, vytvoreni stripped partitions
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

		return this.returnValue;

	}


}
