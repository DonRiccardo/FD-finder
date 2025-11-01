package de.metanome.algorithms.fastfds.fastfds_helper.modules.container;

import it.unimi.dsi.fastutil.longs.LongList;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Represents a stripped partition for a specific attribute in a relational dataset.
 */
public class _StrippedPartitionSpark implements Serializable{

    protected int attribute;
    protected List<LongList> value = new LinkedList<LongList>();

    /**
     * Create a StrippedPartition for specified attrinute.
     *
     * @param attribute {@link Integer} attribute ID
     */
    public _StrippedPartitionSpark(int attribute) {

        this.attribute = attribute;
    }

    /**
     * Create a StrippedPartition from a List containing stripped partitions.
     *
     * @param sp {@link List} of stripped partitions
     */
    public _StrippedPartitionSpark(List<LongList> sp) {

        this.value.addAll(sp);
    }

    /**
     * Add a new equivalence class to this partition.
     *
     * @param element {@link LongList} of tuple IDs
     */
    public void addElement(LongList element) {

        this.value.add(element);
    }

    /**
     * Get the attribute ID.
     *
     * @return {@link Integer} attribute ID
     */
    public int getAttributeID() {

        return this.attribute;
    }

    /**
     * Get the list of equivalence classes (stripped partition values).
     *
     * @return {@link List} of LongLists
     */
    public List<LongList> getValues() {

        return this.value;
    }

    /**
     * @return {@link String} representation of this StrippedPartition for debugging.
     */
    protected String toString_() {

        String s = "sp(";
        for (LongList il : this.value) {
            s += il.toString() + "-";
        }
        return s + ")";
    }

    /**
     * Creates a shallow copy of this StrippedPartition.
     *
     * @return a new {@link _StrippedPartitionSpark} instance with the same attribute ID and equivalence classes
     */
    public _StrippedPartitionSpark copy() {
        _StrippedPartitionSpark copy = new _StrippedPartitionSpark(this.attribute);
        for (LongList l : this.value) {
            copy.value.add(l);
        }

        return copy;
    }
}
