package de.metanome.algorithms.tane.model;

import java.io.Serializable;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Represents a candidate combination of attributes (LHS) along with the associated stripped partition
 * and a set of possible RHS (dependent) attributes.
 */
public class CombinationHelperSpark implements Serializable{
    private static final long serialVersionUID = 1L;

    private BitSet latticeBuilding;
    private BitSet rhsCandidates;
    private boolean valid;
    private _StrippedPartitionSpark partition;
    //private Map<BitSet, _StrippedPartitionSpark> partitionsOfAncestors;

    /**
     * Default constructor. Initializes the combination as valid.
     */
    public CombinationHelperSpark() {

        valid = true;
        //partitionsOfAncestors = new HashMap<>();
    }

    public CombinationHelperSpark(BitSet actualAttributes, int numOfAttributes){

        this();
        latticeBuilding = createBitSetAndNotAttributes(actualAttributes, numOfAttributes);
        rhsCandidates = createBitSetAndNotAttributes(actualAttributes, numOfAttributes);
    }

    /**
     * Create {@link BitSet} as a complement of the actualAttributes in the domain with the size of numOfAttributes.
     *
     * @param actualAttributes {@link BitSet} actual attributes to be unset in returned {@link BitSet}
     * @param numOfAttributes {@link Integer} total number of possible attributes
     * @return {@link BitSet} complement of {@code  actualAttributes}
     */
    private BitSet createBitSetAndNotAttributes (BitSet actualAttributes, int numOfAttributes){

        BitSet b = new BitSet(numOfAttributes);
        b.set(0, numOfAttributes);
        b.andNot(actualAttributes);

        return b;
    }

    public BitSet getLatticeBuilding() {
        return latticeBuilding;
    }

    public void setLatticeBuilding(int numberAttributes) {
        this.latticeBuilding = new  BitSet(numberAttributes);
        this.latticeBuilding.set(0, numberAttributes);
    }

    public void setLatticeBuilding(BitSet latticeBuilding) {
        this.latticeBuilding = (BitSet) latticeBuilding.clone();
    }

    public void unsetLatticeBuilding(int attribute) {
        this.latticeBuilding.clear(attribute);
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

    /**
     * Store partition for the specified attribute {@link BitSet}.
     * @param b {@link BitSet} of attributes
     * @param sp {@link _StrippedPartitionSpark} of the specified attributes
     */
  /*  public void addPartitionOfAncestor(BitSet b, _StrippedPartitionSpark sp){

        this.partitionsOfAncestors.put(b, sp);
    }
*/
    /**
     * Returns stored {@link _StrippedPartitionSpark} for specified {@link BitSet} or
     * {@code null} if the key is not present or the key is {@code null}.
     * @param b {@link BitSet} of attributes
     * @return stored {@link _StrippedPartitionSpark} or {@code null}
     */
   /* public _StrippedPartitionSpark getPartitionOfAncestor(BitSet b){

        if (b == null) return null;
        return partitionsOfAncestors.get(b);
    }
*/
    /**
     *
     * @return KeySet ({@link BitSet} of the attributes) of the stored partitions
     */
   /* public Set<BitSet> getKeySetPartitionsOfAncestors(){

        return partitionsOfAncestors.keySet();
    }
*/
    /**
     *
     * @return one Key ({@link BitSet}) from the partitions of the ancestor
     */
 /*   public BitSet getOneKeyFromPartitionOfAncestors(){

        return partitionsOfAncestors.keySet().iterator().next();
    }

  */
}
