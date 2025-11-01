package de.metanome.algorithms.hyfd.structures;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Represents a collection of functional dependencies (FDs) organized by levels,
 * where each level corresponds to the number of attributes on the left-hand side (LHS)
 * of the dependency.
 *
 * @see BitSet
 * @see FDSet
 */
public class FDList {

	private List<List<BitSet>> fdLevels;
	
	private int depth = 0;
	private int maxDepth;

	/**
	 * Constructs a new {@code FDList} with the given number of attributes and maximum depth.
	 * <p>
	 * Initializes internal FD levels as an array of {@link ArrayList} instances.
	 * </p>
	 *
	 * @param numAttributes the total number of attributes in the dataset
	 * @param maxDepth the maximum allowed level (depth) for this FD list
	 */
	public FDList(int numAttributes, int maxDepth) {
		this.maxDepth = maxDepth;
		this.fdLevels = new ArrayList<List<BitSet>>(numAttributes);
		for (int i = 0; i <= numAttributes; i++)
			this.fdLevels.add(new ArrayList<BitSet>());
	}

	/**
	 * Returns all FD levels maintained by this list.
	 * <p>
	 * Each level corresponds to FDs with the same number of LHS attributes.
	 * </p>
	 *
	 * @return a list of FD levels, each level represented by a {@link List} of {@link BitSet} objects
	 */
	public List<List<BitSet>> getFdLevels() {

		return this.fdLevels;
	}

	/**
	 * Returns the current maximum level (depth) of FDs stored in this list.
	 *
	 * @return the highest level containing any FDs
	 */
	public int getDepth() {

		return this.depth;
	}

	/**
	 * Returns the maximum allowed depth for this FD list.
	 *
	 * @return the configured maximum FD level
	 */
	public int getMaxDepth() {

		return this.maxDepth;
	}

	/**
	 * Adds a new functional dependency to this FD list.
	 * <p>
	 * The FD is inserted into the level corresponding to its number of attributes
	 * (determined by {@link BitSet#cardinality()}).
	 * </p>
	 *
	 * @param fd the {@link BitSet} representing the functional dependency to add
	 * @return {@code true} if the FD was successfully added, {@code false} if it exceeds the maximum depth
	 */
	public boolean add(BitSet fd) {
		int length = fd.cardinality();
		
		if ((this.maxDepth > 0) && (length > this.maxDepth))
			return false;
		
		this.depth = Math.max(this.depth, length);
		return this.fdLevels.get(length).add(fd);
	}

	/**
	 * Trims this FD list to the specified maximum depth.
	 * <p>
	 * All levels deeper than {@code newDepth} are removed.
	 * </p>
	 *
	 * @param newDepth the new maximum FD depth to retain
	 */
	public void trim(int newDepth) {
		while (this.fdLevels.size() > (newDepth + 1)) // +1 because uccLevels contains level 0
			this.fdLevels.remove(this.fdLevels.size() - 1);
		
		this.depth = newDepth;
		this.maxDepth = newDepth;
	}

	/**
	 * Clears all functional dependencies from this FD list.
	 * <p>
	 * Reinitializes the internal structure while preserving the original number of levels.
	 * </p>
	 */
	public void clear() {
		int numLevels = this.fdLevels.size();
		this.fdLevels = new ArrayList<List<BitSet>>(numLevels);
		for (int i = 0; i <= numLevels; i++)
			this.fdLevels.add(new ArrayList<BitSet>());
	}

	/**
	 * Returns the total number of functional dependencies stored in this FD list.
	 * <p>
	 * This counts all FDs across all levels.
	 * </p>
	 *
	 * @return the total number of FDs contained in this list
	 */
	public int size() {
		int size = 0;
		for (List<BitSet> uccs : this.fdLevels)
			size += uccs.size();
		return size;
	}
}
