package cz.cuni.mff.fdfinder.fastfdsservice.algorithm;

import cz.cuni.mff.fdfinder.fastfdsservice.algorithm.model._Input;
import cz.cuni.mff.fdfinder.fastfdsservice.algorithm.model._DifferenceSet;
import cz.cuni.mff.fdfinder.fastfdsservice.algorithm.model._StrippedPartitionSpark;
import cz.cuni.mff.fdfinder.fastfdsservice.algorithm.model._TupleEquivalenceClassRelation;
import cz.cuni.mff.fdfinder.fastfdsservice.algorithm.service._DifferenceSetFromAgreeSetGenerator;
import cz.cuni.mff.fdfinder.fastfdsservice.algorithm.service._FindCoversGenerator;
import cz.cuni.mff.fdfinder.fastfdsservice.algorithm.service._StrippedPartitionGenerator;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

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

    public void execute() {

        JavaPairRDD<Integer, _StrippedPartitionSpark> strippedPartitions = new _StrippedPartitionGenerator(relationships).execute(input);

        JavaRDD<_DifferenceSet> diff = new _DifferenceSetFromAgreeSetGenerator(relationships, input.numberOfColumns()).executeBottleneck(strippedPartitions);

        new _FindCoversGenerator(input, maxLhs).execute(diff);


    }

    
}
