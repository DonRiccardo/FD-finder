package de.metanome.algorithms.depminer.depminer_algorithm.modules;

import de.metanome.algorithms.depminer.depminer_helper.modules.container.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * Generates maximal sets (MAX_SET) and complement maximal sets (CMAX_SET).
 */
public class _CMAX_SET_Generator implements Serializable{

	private Map<Integer, _MAX_SET> maxSet;
	private List<_CMAX_SET> cmaxSet;

	private JavaRDD<_AgreeSet> agreeSets;
	private int numberOfAttributes;
    private JavaPairRDD<Integer, _MAX_SET> maxSetRDD;

    /**
     * Constructor.
     *
     * @param agreeSets {@link JavaRDD} of agree sets
     * @param numberOfAttributes {@link Integer} total number of attributes in the relation
     */
	public _CMAX_SET_Generator(JavaRDD<_AgreeSet> agreeSets, int numberOfAttributes) {
		this.agreeSets = agreeSets;
		this.numberOfAttributes = numberOfAttributes;

	}

    /**
     * Used to define a specific target FD manually for debugging or testing.
     * Not actively used in generation.
     *
     * @param columnIndex {@link Integer} dependent attribute
     * @param bits {@link Integer} indices of attributes in the LHS
     */
	public void targetFD(int columnIndex, int... bits) {

		_AgreeSet s = new _AgreeSet();
		for (int index = 0; index < bits.length; ++index) {
			s.add(bits[index]);
		}
	}

    /**
     * Generates the MAX_SETs for each attribute in the relation.
     * @return {@link JavaPairRDD} of (attribute {@link Integer} attribute → corresponding {@link _MAX_SET})
     */
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

    /**
     * Generates the C-MAX sets for each attribute.
     *
     * @return {@link JavaPairRDD} of (attribute {@link Integer} attribute → corresponding {@link _CMAX_SET})
     */
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
