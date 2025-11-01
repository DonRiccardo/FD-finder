package de.metanome.algorithms.hyfd.structures;

import de.metanome.algorithms.hyfd.utils.ValueComparator;

import java.util.ArrayList;
import java.util.BitSet;

/**
 * Represents a node in a Non-Functional Dependency (NonFD) tree.
 * Each node has children corresponding to attributes. A path from the root
 * to a node marked as `end` represents a set of attributes forming a NonFD.
 */
public class NonFDTreeElement {

	protected NonFDTreeElement[] children;
	protected boolean end = false;

	/**
	 * Constructs a NonFDTreeElement with a given number of attributes.
	 *
	 * @param numAttributes number of attributes (defines the size of children array)
	 */
	public NonFDTreeElement(int numAttributes) {

		this.children = new NonFDTreeElement[numAttributes];
	}

	/**
	 * Adds a pair of records to the NonFD tree if they differ in some attribute.
	 * Recursively descends the tree and creates nodes as necessary.
	 *
	 * @param t1 first record represented as an int array
	 * @param t2 second record represented as an int array
	 * @param valueComparator comparator to determine differences between attribute values
	 * @param attribute current attribute index being examined
	 * @param newNonFD whether a new NonFD has already been discovered
	 * @return {@code true} if a new NonFD node was created, {@code false} otherwise
	 */
	public boolean addMatches(int[] t1, int[] t2, ValueComparator valueComparator, int attribute, boolean newNonFD) {
		do {
			attribute++;
			if (attribute == t1.length) {
				this.end = true;
				return newNonFD;
			}
		}
		while (valueComparator.isDifferent(t1[attribute], t2[attribute]));
		
		if (this.children[attribute] == null) {
			this.children[attribute] = new NonFDTreeElement(this.children.length);
			newNonFD = true;
		}
		
		return this.children[attribute].addMatches(t1, t2, valueComparator, attribute, newNonFD);
	}

	/**
	 * Recursively collects all NonFD paths from this node into a list of BitSets.
	 * Each BitSet represents the attributes of a NonFD.
	 *
	 * @param bitsets list of BitSets to collect NonFDs
	 * @param bitset current BitSet representing the path so far
	 * @param thisAttribute index of the current attribute in the recursion
	 */
	public void asBitSets(ArrayList<BitSet> bitsets, BitSet bitset, int thisAttribute) {
		bitset.set(thisAttribute);
		
		if (this.end)
			bitsets.add((BitSet) bitset.clone());
		
		for (int i = thisAttribute; i < this.children.length; i++)
			if (this.children[i] != null)
				this.children[i].asBitSets(bitsets, bitset, i);
		
		bitset.clear(thisAttribute);
	}
	
	
}
