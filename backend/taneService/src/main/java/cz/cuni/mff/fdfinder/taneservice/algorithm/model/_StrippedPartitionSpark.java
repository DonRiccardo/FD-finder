/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package cz.cuni.mff.fdfinder.taneservice.algorithm.model;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
//import scala.Serializable;

/**
 *
 * @author Richard
 */
public class _StrippedPartitionSpark implements Serializable{
    private double error;
    private long elementCount;
    private final List<LongList> value = new LinkedList<>();
    //private ObjectBigArrayBigList<LongBigArrayBigList> strippedPartition = null;

    /**
     * Create a StrippedPartition with only one equivalence class with the definied number of elements. <br/>
     * Tuple ids start with 0 to numberOfElements-1
     *
     * @param numberTuples
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

    public _StrippedPartitionSpark(List<LongList> sp, long elementCount) {
        this.value.addAll(sp);
        this.elementCount = elementCount;
        this.calculateError();

    }

    public double getError() {
        return error;
    }
    public long getElemCount(){
        return elementCount;
    }

    public List<LongList> getStrippedPartition() {
        return this.value;
    }

    private void calculateError() {
        // calculating the error. Dividing by the number of entries
        // in the whole population is not necessary.
        this.error = this.elementCount - this.value.size();
    }

    public void empty() {
        this.value.clear();
        this.elementCount = 0;
        this.error = 0.0;
    }
}
