package de.metanome.algorithms.fastfds.modules.container;

import de.metanome.algorithms.fastfds.fastfds_helper.modules.container._AgreeSet;

import java.util.BitSet;

/**
 * Represents a Difference Set for Functional Dependency computation.
 */
public class _DifferenceSet extends _AgreeSet {

    /**
     * Construct a DifferenceSet with a predefined BitSet.
     *
     * @param obs {@link BitSet} representing the attributes in the difference set
     */
    public _DifferenceSet(BitSet obs) {

        this.attributes = obs;
    }

    /**
     * Default constructor initializes an empty DifferenceSet.
     */
    public _DifferenceSet() {

        this(new BitSet());
    }
    
    @Override
    public String toString() {

        //return "diff(" + _BitSetUtil.convertToIntList(this.attributes).toString() + ")";
        return "diff("+this.attributes+")";
    }
}
