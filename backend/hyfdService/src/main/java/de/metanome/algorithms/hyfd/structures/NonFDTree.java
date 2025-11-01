package de.metanome.algorithms.hyfd.structures;

import de.metanome.algorithms.hyfd.utils.ValueComparator;

import java.util.ArrayList;
import java.util.BitSet;

/**
 * Represents the root of a Non-Functional Dependency (NonFD) tree.
 * Extends {@link NonFDTreeElement} to provide size tracking.
 */
public class NonFDTree extends NonFDTreeElement {

	int size = 0;

	/**
	 * Returns the number of NonFDs currently stored in the tree.
	 *
	 * @return number of NonFDs
	 */
	public int size() {

		return this.size;
	}

	/**
	 * Constructs a NonFDTree for a given number of attributes.
	 *
	 * @param numAttributes number of attributes in the dataset
	 */
	public NonFDTree(int numAttributes) {

		super(numAttributes);
	}

	/**
	 * Adds a pair of records to the NonFD tree if they differ in some attribute.
	 * Updates the size of the tree if a new NonFD is discovered.
	 *
	 * @param t1 first record represented as an int array
	 * @param t2 second record represented as an int array
	 * @param valueComparator comparator to determine differences between attribute values
	 * @return {@code true} if a new NonFD was created, {@code false} otherwise
	 */
	public boolean addMatches(int[] t1, int[] t2, ValueComparator valueComparator) {
		int attribute = 0;
		boolean newNonFD = false;
		
		while (valueComparator.isDifferent(t1[attribute], t2[attribute])) {
			attribute++;
			if (attribute == t1.length)
				return newNonFD;
		}
		
		if (this.children[attribute] == null) {
			this.children[attribute] = new NonFDTreeElement(this.children.length);
			newNonFD = true;
		}
		
		newNonFD = this.children[attribute].addMatches(t1, t2, valueComparator, attribute, newNonFD);
		if (newNonFD)
			this.size++;
		
		return newNonFD;
	}

	/**
	 * Returns all NonFDs stored in the tree as a list of BitSets.
	 * Each BitSet represents a NonFD with bits set corresponding to attribute indices.
	 *
	 * @return list of BitSets representing all NonFDs
	 */
	public ArrayList<BitSet> asBitSets() {
		ArrayList<BitSet> bitsets = new ArrayList<>(this.size);
		BitSet bitset = new BitSet(this.children.length);
		
		for (int i = 0; i < this.children.length; i++)
			if (this.children[i] != null)
				this.children[i].asBitSets(bitsets, bitset, i);
		
		return bitsets;
	}
}
