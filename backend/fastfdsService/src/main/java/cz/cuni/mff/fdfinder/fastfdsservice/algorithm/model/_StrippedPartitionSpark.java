package cz.cuni.mff.fdfinder.fastfdsservice.algorithm.model;

import it.unimi.dsi.fastutil.longs.LongList;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public class _StrippedPartitionSpark implements Serializable{

    protected int attribute;
    protected List<LongList> value = new LinkedList<LongList>();
    //private long elementCount;
    protected boolean finalized = false;

    public _StrippedPartitionSpark(int attribute) {

        this.attribute = attribute;
    }
    
    public _StrippedPartitionSpark(List<LongList> sp, long elementCount) {
        this.value.addAll(sp);
       // this.elementCount = elementCount;

    }

    public void addElement(LongList element) {

        if (finalized) {
            return;
        }
        this.value.add(element);
    }
/*
    public void finalize() {

        this.finalized = true;
    }
*/
    public int getAttributeID() {

        return this.attribute;
    }

    public List<LongList> getValues() {

        return this.value;

        // if (finalized) {
        // // TODO: notwendig?
        // List<IntList> temp = new LinkedList<IntList>();
        // for (IntList il : this.value) {
        // IntList temp_ = new LongArrayList();
        // temp_.addAll(il);
        // temp.add(temp_);
        // }
        // return temp;
        // } else {
        // return this.value;
        // }
    }

    /*
    public List<BitSet> getValuesAsBitSet() {

        List<BitSet> result = new LinkedList<BitSet>();
        for (LongList list : this.value) {
            BitSet set = new BitSet();
            for (long i : list) {
                set.set(i);
            }
            result.add(set);
        }
        return result;
    }
*/

    protected String toString_() {

        String s = "sp(";
        for (LongList il : this.value) {
            s += il.toString() + "-";
        }
        return s + ")";
    }

    public _StrippedPartitionSpark copy() {
        _StrippedPartitionSpark copy = new _StrippedPartitionSpark(this.attribute);
        for (LongList l : this.value) {
            copy.value.add(l);
        }
        copy.finalized = this.finalized;
        return copy;
    }
}
