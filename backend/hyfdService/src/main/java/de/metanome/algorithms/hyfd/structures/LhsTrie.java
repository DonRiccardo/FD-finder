package de.metanome.algorithms.hyfd.structures;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Represents a trie structure for storing left-hand sides (LHSs) of functional
 * dependencies (FDs). Extends {@link LhsTrieElement} with methods to add,
 * remove, and query LHSs.
 */
public class LhsTrie extends LhsTrieElement {

	protected int numAttributes;

	/**
	 * Constructs an empty LHS trie with the specified number of attributes.
	 *
	 * @param numAttributes total number of attributes
	 */
	public LhsTrie(int numAttributes) {

		this.numAttributes = numAttributes;
	}

	/**
	 * Adds a left-hand side (LHS) to the trie. Creates intermediate nodes
	 * as necessary.
	 *
	 * @param lhs BitSet representing the attributes in the LHS
	 * @return the {@link LhsTrieElement} corresponding to the added LHS
	 */
	public LhsTrieElement addLhs(BitSet lhs) {
		LhsTrieElement currentNode = this;
		for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
			if (currentNode.getChildren()[i] != null)
				currentNode.setChild(this.numAttributes, i, new LhsTrieElement());
			currentNode = currentNode.getChildren()[i];
		}
		return currentNode;
	}

	/**
	 * Removes the specified LHS from the trie. Cleans up intermediate nodes
	 * if they have no other children.
	 *
	 * @param lhs BitSet representing the attributes in the LHS to remove
	 */
	public void removeLhs(BitSet lhs) {
		LhsTrieElement[] path = new LhsTrieElement[lhs.cardinality()];
		int currentPathIndex = 0;
		
		LhsTrieElement currentNode = this;
		path[currentPathIndex] = currentNode;
		currentPathIndex++;
		
		for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
			currentNode = currentNode.getChildren()[i];
			path[currentPathIndex] = currentNode;
			currentPathIndex++;
		}
		
		for (int i = path.length - 1; i >= 0; i --) {
			path[i].removeChild(i);
			if (path[i].getChildren() != null)
				break;
		}
	}

	/**
	 * Returns all LHSs in the trie that match or generalize the specified LHS.
	 *
	 * @param lhs BitSet representing the LHS to query
	 * @return a list of BitSets representing the found LHSs and generalizations
	 */
	public List<BitSet> getLhsAndGeneralizations(BitSet lhs) {
		List<BitSet> foundLhs = new ArrayList<>();
		BitSet currentLhs = new BitSet();
		int nextLhsAttr = lhs.nextSetBit(0);
		this.getLhsAndGeneralizations(lhs, nextLhsAttr, currentLhs, foundLhs);
		return foundLhs;
	}

	/**
	 * Checks whether the trie contains the specified LHS or any of its
	 * generalizations.
	 *
	 * @param lhs BitSet representing the LHS to check
	 * @return true if the LHS or a generalization exists, false otherwise
	 */
	public boolean containsLhsOrGeneralization(BitSet lhs) {
		int nextLhsAttr = lhs.nextSetBit(0);
		return this.containsLhsOrGeneralization(lhs, nextLhsAttr);
	}

	/**
	 * Returns all LHSs stored in the trie as a list of BitSets.
	 *
	 * @return list of BitSets representing all LHSs in the trie
	 */
	public List<BitSet> asBitSetList() {
		List<BitSet> foundLhs = new ArrayList<>();
		BitSet currentLhs = new BitSet();
		int nextLhsAttr = 0;
		this.asBitSetList(currentLhs, nextLhsAttr, foundLhs);
		return foundLhs;
	}
}
