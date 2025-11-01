package de.metanome.algorithms.depminer.depminer_helper.modules;

import de.metanome.algorithms.depminer.depminer_helper.modules.container._AgreeSet;
import de.metanome.algorithms.depminer.depminer_helper.modules.container._StrippedPartition;
import cz.cuni.mff.fdfinder.depminerservice.algorithm.util._LongBitSet;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.*;

/**
 * Generates {@link _AgreeSet} objects from stripped partitions.
 * @author pavel.koupil
 */
public class _AgreeSetGenerator {

	private static final Boolean DEBUG = Boolean.FALSE;
        private static final int SUBLIST_SIZE = 50;

	public _AgreeSetGenerator() {
            
	}

    /**
     * Comparator for sorting entries by the size and lexicographic order of LongList elements.
     */
    private static class ListComparator2 implements Comparator<Tuple2<List<Integer>, LongList>> {

        @Override
        public int compare(Tuple2<List<Integer>, LongList> x, Tuple2<List<Integer>, LongList> y) {
                        LongList l1 = x._2;
                        LongList l2 = y._2;

            if (l1.size() - l2.size() != 0) {
                return l2.size() - l1.size();
            }
            for (int i = 0; i < l1.size(); i++) {
                if (l1.getLong(i) == l2.getLong(i)) {
                    continue;
                }
                return (int) (l2.getLong(i) - l1.getLong(i));
            }
            return 0;
        }

    }

    /**
     * Comparator ordering by descending size and lexicographically by tuple IDs.
     */
    private static class ListComparatorBottleneck implements Comparator<Tuple2<BitSet, LongList>> {

        @Override
        public int compare(Tuple2<BitSet, LongList> x, Tuple2<BitSet, LongList> y) {
                        LongList l1 = x._2;
                        LongList l2 = y._2;

            if (l1.size() - l2.size() != 0) {
                return l2.size() - l1.size();
            }
            for (int i = 0; i < l1.size(); i++) {
                if (l1.getLong(i) == l2.getLong(i)) {
                    continue;
                }
                return (int) (l2.getLong(i) - l1.getLong(i));
            }
            return 0;
        }

	}

    /**
     * Generates agree sets using a direct Cartesian-based approach.
     *
     * @param partitions {@link JavaPairRDD} of (attribute {@link Integer} attribute → {@link _StrippedPartition})
     * @return {@link JavaRDD} of computed {@link _AgreeSet} objects
     */
	public JavaRDD<_AgreeSet> execute(JavaPairRDD<Integer, _StrippedPartition> partitions) throws Exception {

		if (_AgreeSetGenerator.DEBUG) {
			long sum = 0;
			for (_StrippedPartition p : partitions.collectAsMap().values()) {
				System.out.println("-----");
				System.out.println("Atribut: " + p.getAttributeID());
				System.out.println("Pocet oddilu: " + p.getValues().size());
				sum += p.getValues().size();
			}
			System.out.println("-----");
			System.out.println("Celkem: " + sum);
			System.out.println("-----");
		}

		JavaPairRDD<Integer, _LongBitSet> equivalenceClasses = partitions
                        .flatMapToPair(tuple -> {
                            List<Tuple2<Integer, _LongBitSet>> result = new ArrayList<Tuple2<Integer, _LongBitSet>>();
                            for (_LongBitSet listValues : tuple._2.getValuesAsBitSet()){
                                result.add(new Tuple2<>(tuple._1, listValues));
                            }
                            return result.iterator();
                        });
                
                JavaRDD<_AgreeSet> resultAgreeSet = equivalenceClasses
                        .cartesian(equivalenceClasses)
                        // vytvorenie bitSetu (trieda ekvivalencie) a boolean isSubset (či je tuple._1 podmnozinou tuple._2)
                        .mapToPair(tuple -> {
                            boolean isSubset = false;
                            
                            if (!tuple._1._2.equals(tuple._2._2)){
                                tuple._2._2.and(tuple._1._2);
                                isSubset = (tuple._1._2.equals(tuple._2._2));
                            }
                            if(_AgreeSetGenerator.DEBUG) System.out.println("AG--SUBSET:" + tuple._1._2 + tuple._2._2 + isSubset);
                            return new Tuple2<>(tuple._1._2, isSubset);
                        })
                        .reduceByKey((x, y) -> {
                            boolean combined = x || y;
                            return combined;
                        })
                        .filter(x -> !x._2)
                        .flatMap(tuple -> {
                            List<Tuple2<Long, Long>> result = new ArrayList<>();
                            
                            for (long i = tuple._1.nextSetBit(0); i >= 0; i = tuple._1.nextSetBit(i+1)) {
                                for (long j = tuple._1.nextSetBit(i+1); j >= 0; j = tuple._1.nextSetBit(j+1)) {
                                    result.add(new Tuple2<>(i,j));
                                }
                            }
                            
                            return result.iterator();
                        })
                        .cartesian(equivalenceClasses)
                        .mapToPair(tuple -> {
                            BitSet attributes = new BitSet();
                            if (tuple._2._2.get(tuple._1._1) && tuple._2._2.get(tuple._1._2)){
                                attributes.set(tuple._2._1);
                            }
                            if (_AgreeSetGenerator.DEBUG) System.out.println("AG--CONTAINS: "+ tuple._1._1 + tuple._1._2 + tuple._2._2);
                            return new Tuple2<>(tuple._1, attributes);
                        })
                        .reduceByKey((x, y) -> {
                            BitSet combined = (BitSet) x.clone();
                            combined.or(y);
                            return combined;
                        })
                        .map(tuple -> {
                            _AgreeSet ag = new _AgreeSet();
                            ag.setAttributes(tuple._2);
                            return ag;
                        })
                        .distinct();


		return resultAgreeSet;
	}

    /**
     * Generate {@link _AgreeSet} based on compusting Maximal Sets from {@link _StrippedPartition}
     * @param partitions {@link JavaPairRDD} of {@link BitSet} and {@link _StrippedPartition} of input dataset
     * @return computed {@link _AgreeSet}
     */
    public JavaRDD<_AgreeSet> executeBottleneck(JavaPairRDD<BitSet, _StrippedPartition> partitions){
        JavaRDD<_AgreeSet> agreeSets = partitions
            .flatMapToPair(tuple -> {
                // Boolean iba ako Key na zjednotenie vsetkych _StrippedPartitions
                List<Tuple2<Boolean, List<Tuple2<BitSet, LongList>>>> result = new ArrayList<>();

                for (LongList listValues : tuple._2.getValues()){
                    LinkedList<Tuple2<BitSet, LongList>> partitionsIDs = new LinkedList();
                    listValues.sort(null); //natural ordering sort IDs of tuples

                    BitSet attributes = (BitSet) tuple._1.clone();
                    partitionsIDs.add(new Tuple2<>(attributes, listValues));

                    result.add(new Tuple2<>(true, partitionsIDs));
                }
                return result.iterator();
            })
            // merge all _StrippedPartitions
            .reduceByKey((x, y) -> {
                if (x.size() < y.size()){
                    y.addAll(x);
                    return y;
                }

                x.addAll(y);
                return x;
            })
            // vypocet MAX mnozin ako bottleneck!
            .flatMapToPair(tuple -> {
                TreeSet<Tuple2<BitSet, LongList>> treePartitions = new TreeSet<>(new ListComparatorBottleneck());
                TreeSet<Tuple2<BitSet, LongList>> maxSets = new TreeSet<>(new ListComparatorBottleneck());

                for (Tuple2<BitSet, LongList> IDs : tuple._2){
                    treePartitions.add(IDs);
                }

                // vlozit najvacsiu mnozinu do maxSets, pretoze urcite nebude podmnozinou
                maxSets.add(treePartitions.pollFirst());

                for (Tuple2<BitSet, LongList> element : treePartitions){

                    int elementSize = element._2.size();
                    boolean isSubset = false;

                    for (Tuple2<BitSet, LongList> maxElement : maxSets){
                        int maxElementSize = maxElement._2.size();

                        // heuristika, ci mensia mnozina má IDs v intervale vacsej mnoziny a taktiez ma mensi/rovny pocet IDs ako vacsia mnozina
                        if (element._2.get(0) >= maxElement._2.get(0)
                            && element._2.get(elementSize - 1) <= maxElement._2.get(maxElementSize - 1)
                            && elementSize <= maxElementSize){

                            if (maxElement._2.containsAll(element._2)){
                                isSubset = true;
                                maxElement._1.or(element._1);
                                break;
                            }

                        }
                        // overovanie dalsich mnozin v maxSets nema zmysel, pretoze nemozu byt nadmnozinou
                        else if (elementSize > maxElementSize) {
                            break;
                        }

                    }

                    if (!isSubset){
                        maxSets.add(element);
                    }
                }
                System.out.println("MAXsets size: "+ maxSets.size());
                return maxSets.iterator();
            })
            // tuple as <Attributes which SP are subsets of this MaxSet, MaxSet>
            .flatMapToPair(tuple -> {
                List<Tuple2<BitSet, Tuple2<LongList, LongList>>> result = new LinkedList<>();
                int longListSize = tuple._2.size();
                for (int i = 0; i < longListSize; i += _AgreeSetGenerator.SUBLIST_SIZE){
                    int toIndex = Math.min(longListSize, i + _AgreeSetGenerator.SUBLIST_SIZE);

                    LongList subList = new LongArrayList(tuple._2.subList(i, toIndex));

                    for (int j = tuple._1.nextSetBit(0); j != -1; j = tuple._1.nextSetBit(j + 1)){
                        BitSet b = new BitSet();
                        b.set(j);
                        result.add(new Tuple2<>(b, new Tuple2<>(subList, tuple._2 )));
                    }
                }

                return result.iterator();
            })
            .join(partitions)
            // vytvoria sa dvojice <ID, ID> iba pre tie ID, ktore sa nachadzaju v rovnakej triede ekvivalencie daneho atributu
            // flatMap a filter v jednom kroku
            .flatMapToPair(tuple -> {
                List<Tuple2<Tuple2<Long, Long>, BitSet>> result = new LinkedList<>();
                //System.out.println("Chcecking pair of IDs in ATT: "+tuple._1);
                for (int first = 0; first < tuple._2._1._1.size(); first++){
                    for (int second = 0; second < tuple._2._1._2.size(); second++){

                        if (tuple._2._2.isFirstInSamePartitionAsSecond(tuple._2._1._1.get(first), tuple._2._1._2.get(second))){
                            result.add(new Tuple2<>(new Tuple2<>(tuple._2._1._1.get(first), tuple._2._1._2.get(second)), (BitSet) tuple._1.clone()));

                        }
                    }
                }

                return result.iterator();
            })
            .reduceByKey((x, y) -> {
                BitSet combined = (BitSet)x.clone();
                combined.or(y);
                return combined;
            })
            .map(tuple -> {
                _AgreeSet ag = new _AgreeSet();
                ag.setAttributes(tuple._2);
                return ag;
            })
            .distinct();

        return agreeSets;

    }
        
        
        
}
