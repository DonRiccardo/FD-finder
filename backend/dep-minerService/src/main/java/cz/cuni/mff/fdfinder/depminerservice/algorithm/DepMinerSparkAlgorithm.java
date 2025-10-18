/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package cz.cuni.mff.fdfinder.depminerservice.algorithm;

import cz.cuni.mff.fdfinder.depminerservice.algorithm.model.*;
import cz.cuni.mff.fdfinder.depminerservice.algorithm.service.*;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.*;

/**
 *
 * @author pavel.koupil
 */
public class DepMinerSparkAlgorithm {

	private final int maxLhs;
    private final _Input input;

    private _CMAX_SET_Generator setGenerator;

    Int2ObjectMap<List<BitSet>> lhss;

    _FunctionalDependencyGenerator xxx;

    public DepMinerSparkAlgorithm(_Input input, int maxLhs) {

        this.input = input;
        if (maxLhs < 0) {

            this.maxLhs = input.numberOfColumns();
        }
        else {

            this.maxLhs = Math.min(maxLhs, input.numberOfColumns());
        }
    }

    public void execute() throws Exception {

        _StrippedPartitionGenerator spg = new _StrippedPartitionGenerator();
        JavaPairRDD<BitSet, _StrippedPartition> strippedPartitions = spg.execute(input);

        int length = input.numberOfColumns();

        JavaRDD<_AgreeSet> agreeSets = new _AgreeSetGenerator().executeBottleneck(strippedPartitions);

        setGenerator = new _CMAX_SET_Generator(agreeSets, length);
        JavaPairRDD<Integer, _MAX_SET> maxSets = setGenerator.generateMaxSet();

        JavaPairRDD<Integer, _CMAX_SET> cmaxSets = setGenerator.generateCMAX_SETs();

        List<_CMAX_SET> lc = new ArrayList(cmaxSets.collectAsMap().values());
        lhss = new _LeftHandSideGenerator().execute(lc, length, this.maxLhs);
        xxx = new _FunctionalDependencyGenerator(input, input.relationName(), input.columnNames(), lhss);
        List<_FunctionalDependencyGroup> result = xxx.execute();

    }

}
