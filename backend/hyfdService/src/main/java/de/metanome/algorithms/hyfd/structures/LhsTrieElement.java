package de.metanome.algorithms.hyfd.structures;

import java.util.BitSet;
import java.util.List;

/**
 * Represents a single node (element) in a trie structure used to store
 * left-hand sides (LHSs) of functional dependencies (FDs).
 */
public class LhsTrieElement {

	protected LhsTrieElement[] children = null;
	protected int childrenCount = 0;

	/**
	 * Returns the array of children for this trie element.
	 *
	 * @return array of {@link LhsTrieElement} children
	 */
	public LhsTrieElement[] getChildren() {

		return children;
	}

	/**
	 * Sets a child element at the specified index. Initializes the children array
	 * if it is currently null.
	 *
	 * @param numAttributes total number of attributes (size of children array)
	 * @param index index to place the child
	 * @param child child node to set
	 */
	public void setChild(int numAttributes, int index, LhsTrieElement child) {
		if (this.children == null)
			this.children = new LhsTrieElement[numAttributes];
		
		this.children[index] = child;
		this.childrenCount++;
	}

	/**
	 * Removes the child at the specified index. If no children remain after
	 * removal, sets the children array to null.
	 *
	 * @param index index of the child to remove
	 */
	public void removeChild(int index) {
		this.children[index] = null;
		this.childrenCount--;
		
		if (this.childrenCount == 0)
			this.children = null;
	}

	/**
	 * Returns the number of non-null children.
	 *
	 * @return number of children
	 */
	public int getChildrenCount() {

		return childrenCount;
	}

	/**
	 * Sets the children count.
	 *
	 * @param childrenCount number of children
	 */
	public void setChildrenCount(int childrenCount) {

		this.childrenCount = childrenCount;
	}

	/**
	 * Recursively collects all LHSs and their generalizations stored in the
	 * trie starting from this element.
	 *
	 * @param lhs BitSet representing attributes to explore
	 * @param currentLhsAttr current attribute index in recursion
	 * @param currentLhs current LHS being built
	 * @param foundLhs list to collect found LHS BitSets
	 */
	protected void getLhsAndGeneralizations(BitSet lhs, int currentLhsAttr, BitSet currentLhs, List<BitSet> foundLhs) {
		if (this.children == null) {
			foundLhs.add((BitSet) currentLhs.clone());
			return;
		}
		
		while (currentLhsAttr >= 0) {
			int nextLhsAttr = lhs.nextSetBit(currentLhsAttr + 1);
			
			if (this.children[currentLhsAttr] != null) {
				currentLhs.set(currentLhsAttr);
				this.children[currentLhsAttr].getLhsAndGeneralizations(lhs, nextLhsAttr, currentLhs, foundLhs);
				currentLhs.clear(currentLhsAttr);
			}
			
			currentLhsAttr = nextLhsAttr;
		}
	}

	/**
	 * Checks if the trie contains the given LHS or any of its generalizations.
	 *
	 * @param lhs BitSet representing the LHS to check
	 * @param currentLhsAttr current attribute index in recursion
	 * @return true if the LHS or a generalization exists, false otherwise
	 */
	protected boolean containsLhsOrGeneralization(BitSet lhs, int currentLhsAttr) {
		if (this.children == null)
			return true;
		
		// Is the dependency already read and we have not yet found a generalization?
		if (currentLhsAttr < 0)
			return false;
		
		int nextLhsAttr = lhs.nextSetBit(currentLhsAttr + 1);
		
		if (this.children[currentLhsAttr] != null)
			if (this.children[currentLhsAttr].containsLhsOrGeneralization(lhs, nextLhsAttr))
				return true;
		
		return this.containsLhsOrGeneralization(lhs, nextLhsAttr);
	}

	/**
	 * Recursively collects all LHSs stored in the trie as BitSets, starting
	 * from this element.
	 *
	 * @param currentLhs BitSet representing the LHS being built
	 * @param currentLhsAttr current attribute index in recursion
	 * @param foundLhs list to collect found LHS BitSets
	 */
	protected void asBitSetList(BitSet currentLhs, int currentLhsAttr, List<BitSet> foundLhs) {
		if (this.children == null) {
			foundLhs.add((BitSet) currentLhs.clone());
			return;
		}
		
		for (int lhsAttr = currentLhsAttr; lhsAttr < this.children.length; lhsAttr++) {
			if (this.children[lhsAttr] != null) {
				currentLhs.set(lhsAttr);
				this.children[lhsAttr].asBitSetList(currentLhs, lhsAttr + 1, foundLhs);
				currentLhs.clear(lhsAttr);
			}
		}
	}
	
}
