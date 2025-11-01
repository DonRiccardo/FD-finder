package de.metanome.algorithms.fastfds.fastfds_helper.modules.container;

import de.metanome.algorithms.fastfds.fastfds_helper.util._BitSetUtil;

import java.io.Serializable;
import java.util.BitSet;

/**
 * Represents an AgreeSet for attribute.
 */
public class _AgreeSet implements Serializable{//extends StorageSet {

    protected BitSet attributes = new BitSet();

    /**
     * Adds a single attribute to this AgreeSet.
     *
     * @param {@link Integer} attribute the index of the attribute to add
     */
    public void add(int attribute) {

        this.attributes.set(attribute);
    }

    /**
     * Returns a clone of the attributes BitSet.
     *
     * @return {@link BitSet} representing the attributes in this AgreeSet
     */
    public BitSet getAttributes() {

        return (BitSet) this.attributes.clone();

    }

    @Override
    public String toString() {

        return "ag(" + _BitSetUtil.convertToIntList(this.attributes).toString()
                + ")";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((attributes == null) ? 0 : attributes.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        _AgreeSet other = (_AgreeSet) obj;
        if (attributes == null) {
            if (other.attributes != null)
                return false;
        } else if (!attributes.equals(other.attributes))
            return false;
        return true;
    }

}
