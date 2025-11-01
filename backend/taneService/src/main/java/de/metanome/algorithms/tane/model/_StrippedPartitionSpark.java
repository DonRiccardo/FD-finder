package de.metanome.algorithms.tane.model;


import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Represents a stripped partition for a specific attribute in a relational dataset.
 */
public class _StrippedPartitionSpark implements Serializable{
    private double error;
    private long elementCount;
    private final List<LongList> value = new LinkedList<>();

    /**
     * Create a StrippedPartition with only one equivalence class with the definied number of elements.
     *
     * @param numberTuples {@link Long} number of elements
     */
    public _StrippedPartitionSpark(long numberTuples) {
        //this.strippedPartition = new ObjectBigArrayBigList<LongBigArrayBigList>();
        this.elementCount = numberTuples;
        // StrippedPartition only contains partition with more than one elements.
        if (numberTuples > 1) {
            LongArrayList newEqClass = new LongArrayList();
            for (int i = 0; i < numberTuples; i++) {
                newEqClass.add(i);
            }
            this.value.add(newEqClass);
        }
        this.calculateError();
    }

    /**
     * Create a StrippedPartition from a HashMap mapping the values to the tuple ids.
     *
     * @param partition
     */
    public _StrippedPartitionSpark(Object2ObjectOpenHashMap<Object, LongArrayList> partition) {
        //this.strippedPartition = new ObjectBigArrayBigList<LongBigArrayBigList>();
        this.elementCount = 0;

        //create stripped partitions -> only use equivalence classes with size > 1.
        for (LongArrayList eqClass : partition.values()) {
            if (eqClass.size() > 1) {
                value.add(eqClass);
                elementCount += eqClass.size();
            }
        }
        this.calculateError();
    }

    /**
     * Constructs a stripped partition from an existing list of equivalence classes.
     *
     * @param sp the list of equivalence classes (LongLists of tuple IDs)
     * @param elementCount total number of tuples represented
     */
    public _StrippedPartitionSpark(List<LongList> sp, long elementCount) {
        this.value.addAll(sp);
        this.elementCount = elementCount;
        this.calculateError();

    }

    /**
     * Returns the error metric for this partition.
     * @return {@link Double} error = elementCount - number of equivalence classes
     */
    public double getError() {

        return error;
    }

    /**
     * @return {@link Long} the total number of tuples in this partition.
     */
    public long getElemCount(){

        return elementCount;
    }

    /**
     * @return the List of equivalence classes in this stripped partition.
     */
    public List<LongList> getStrippedPartition() {

        return this.value;
    }

    /**
     * Computes the error metric: number of tuples - the number of equivalence classes.
     */
    private void calculateError() {

        this.error = this.elementCount - this.value.size();
    }

    /**
     * Empties the stripped partition, clearing all equivalence classes and resetting counts.
     */
    public void empty() {
        this.value.clear();
        this.elementCount = 0;
        this.error = 0.0;
    }
}
