package de.metanome.algorithms.fastfds;

import cz.cuni.mff.fdfinder.fastfdsservice.algorithm.model._Input;
import de.metanome.algorithms.fastfds.modules.container._DifferenceSet;
import de.metanome.algorithms.fastfds.fastfds_helper.modules.container._StrippedPartitionSpark;
import de.metanome.algorithms.fastfds.fastfds_helper.modules._TupleEquivalenceClassRelation;
import de.metanome.algorithms.fastfds.fastfds_helper.modules._DifferenceSetFromAgreeSetGenerator;
import de.metanome.algorithms.fastfds.modules._FindCoversGenerator;
import de.metanome.algorithms.fastfds.fastfds_helper.modules._StrippedPartitionGenerator;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

/**
 * Class representing actual working algorithm.
 */
public class FastFDsSparkAlgorithm implements Serializable{

    private final _Input input;
    private final int maxLhs;
    private static Long2ObjectOpenHashMap<_TupleEquivalenceClassRelation> relationships = new Long2ObjectOpenHashMap<_TupleEquivalenceClassRelation>();
    
    public FastFDsSparkAlgorithm(_Input input, int maxLhs) {

        this.input = input;
        if (maxLhs < 0) {

            this.maxLhs = input.numberOfColumns();
        }
        else {

            this.maxLhs = Math.min(maxLhs, input.numberOfColumns());
        }
    }

    /**
     * Execute algorithm and found functional dependencies are added to {@link _Input}.
     */
    public void execute() {

        JavaPairRDD<Integer, _StrippedPartitionSpark> strippedPartitions = new _StrippedPartitionGenerator(relationships).execute(input);

        JavaRDD<_DifferenceSet> diff = new _DifferenceSetFromAgreeSetGenerator(relationships, input.numberOfColumns()).executeBottleneck(strippedPartitions);

        new _FindCoversGenerator(input, maxLhs).execute(diff);


    }

    
}
