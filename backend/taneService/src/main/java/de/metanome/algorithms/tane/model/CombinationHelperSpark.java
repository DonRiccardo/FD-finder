package de.metanome.algorithms.tane.model;

import java.io.Serializable;
import java.util.BitSet;

/**
 * Represents a candidate combination of attributes (LHS) along with the associated stripped partition
 * and a set of possible RHS (dependent) attributes.
 */
public class CombinationHelperSpark implements Serializable{
    private static final long serialVersionUID = 1L;

    private BitSet rhsCandidates;
    private boolean valid;

    private _StrippedPartitionSpark partition;

    /**
     * Default constructor. Initializes the combination as valid.
     */
    public CombinationHelperSpark() {

        valid = true;
    }

    /**
     * @return {@link BitSet} RHS candidates
     */
    public BitSet getRhsCandidates() {

        return rhsCandidates;
    }

    /**
     * Sets the RHS candidates for this combination.
     *
     * @param rhsCandidates {@link BitSet} representing candidate dependent attributes
     */
    public void setRhsCandidates(BitSet rhsCandidates) {

        this.rhsCandidates = (BitSet) rhsCandidates.clone();
    }

    /**
     * @return the associated {@link _StrippedPartitionSpark}.
     */
    public _StrippedPartitionSpark getPartition() {

        return partition;
    }

    /**
     * Sets the stripped partition for this combination.
     *
     * @param partition the {@link _StrippedPartitionSpark}
     */
    public void setPartition(_StrippedPartitionSpark partition) {

        this.partition = partition;
    }

    /**
     * @return whether this combination is still valid (not pruned).
     */
    public boolean isValid() {

        return valid;
    }

    /**
     * Marks this combination as invalid and clears its associated partition.
     * Once invalid, this combination will not be considered further in FD discovery.
     */
    public void setInvalid() {
        this.valid = false;
        partition = null;
    }
}
