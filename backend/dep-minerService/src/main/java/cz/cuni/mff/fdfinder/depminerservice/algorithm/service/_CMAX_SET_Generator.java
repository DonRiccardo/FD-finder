/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package cz.cuni.mff.fdfinder.depminerservice.algorithm.service;

import cz.cuni.mff.fdfinder.depminerservice.algorithm.model._AgreeSet;
import cz.cuni.mff.fdfinder.depminerservice.algorithm.model._CMAX_SET;
import cz.cuni.mff.fdfinder.depminerservice.algorithm.model._MAX_SET;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 *
 * @author pavel.koupil
 */
public class _CMAX_SET_Generator implements Serializable{

	private Map<Integer, _MAX_SET> maxSet;
	private List<_CMAX_SET> cmaxSet;

	private JavaRDD<_AgreeSet> agreeSets;
	private int numberOfAttributes;
        private JavaPairRDD<Integer, _MAX_SET> maxSetRDD;

	public _CMAX_SET_Generator(JavaRDD<_AgreeSet> agreeSets, int numberOfAttributes) {
		this.agreeSets = agreeSets;
		this.numberOfAttributes = numberOfAttributes;

	}

	public void targetFD(int columnIndex, int... bits) {
		//var _maxSet = maxSet.get(columnIndex);
		_AgreeSet s = new _AgreeSet();
		for (int index = 0; index < bits.length; ++index) {
			s.add(bits[index]);
		}
		//_maxSet.addCombination(s.getAttributes());
//		_maxSet.finalize_RENAME_THIS();
	}



	public JavaPairRDD<Integer, _MAX_SET> generateMaxSet() {

		this.maxSetRDD = agreeSets
                        .flatMapToPair(ag -> {
                            List<Tuple2<Integer, _AgreeSet>> result = new ArrayList<>();
                            for (int i = 0; i < numberOfAttributes; i++) {
                                result.add(new Tuple2<>(i, ag));
                            }
                            return result.iterator();
                        })
                        .filter(tuple -> !tuple._2.getAttributes().get(tuple._1))
                        .groupByKey()
                        .mapToPair(tuple -> {
                            _MAX_SET mset = new _MAX_SET(tuple._1);
                            for (_AgreeSet agset : tuple._2){
                                mset.addCombination(agset.getAttributes());
                            }
                            
                            mset.finalize_RENAME_THIS();
                            return new Tuple2<>(tuple._1, mset);
                        })
                        ;
                
                return maxSetRDD;
	}

	

	public JavaPairRDD<Integer, _CMAX_SET> generateCMAX_SETs() {
		
            if(this.maxSetRDD == null) this.generateMaxSet();
            
            JavaPairRDD<Integer, _CMAX_SET> cmaxSetRDD = this.maxSetRDD
                    .flatMapToPair(tuple -> {
                        List<Tuple2<Integer, BitSet>> result = new ArrayList<>();
                        for (BitSet bset : tuple._2.getCombinations()) {
                            result.add(new Tuple2<>(tuple._1, bset));                    
                        }
                        return result.iterator();
                    })
                    .mapToPair(tuple -> {
                        BitSet complement = new BitSet();
                        complement.set(0, numberOfAttributes, true);
                        complement.xor(tuple._2);
                        //System.out.println("CMAX--GEN--complement "+ tuple._2 + " c:"+complement);
                        return new Tuple2<>(tuple._1, complement);
                    })
                    .groupByKey()
                    .mapToPair(tuple -> {
                        _CMAX_SET mset = new _CMAX_SET(tuple._1);
                        for (BitSet bset : tuple._2){
                            mset.addCombination(bset);
                        }

                        mset.finalize_RENAME_THIS();
                        return new Tuple2<>(tuple._1, mset);
                    });

		return cmaxSetRDD;
	}

	

}
