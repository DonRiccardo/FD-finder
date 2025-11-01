package de.metanome.algorithms.hyfd.structures;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Represents a set of functional dependencies (FDs) organized by levels,
 * where each level corresponds to the cardinality (number of attributes)
 * of the left-hand side of an FD.
 */
public class FDSet implements Serializable {

	private List<ObjectOpenHashSet<BitSet>> fdLevels;
	
	private int depth = 0;
	private int maxDepth;

	/**
	 * Constructs a new {@code FDSet} for a given number of attributes and maximum depth.
	 *
	 * @param numAttributes the total number of attributes in the dataset
	 * @param maxDepth the maximum allowed depth for this FD set
	 */
	public FDSet(int numAttributes, int maxDepth) {
		this.maxDepth = maxDepth;
		this.fdLevels = new ArrayList<ObjectOpenHashSet<BitSet>>(numAttributes);
		for (int i = 0; i <= numAttributes; i++)
			this.fdLevels.add(new ObjectOpenHashSet<BitSet>());
	}

	/**
	 * Returns all FD levels managed by this set.
	 *
	 * @return a list of {@link ObjectOpenHashSet} instances, where each level stores FDs of the same length
	 */
	public List<ObjectOpenHashSet<BitSet>> getFdLevels() {

		return this.fdLevels;
	}

	/**
	 * Returns the current maximum depth of stored functional dependencies.
	 *
	 * @return the highest level currently containing FDs
	 */
	public int getDepth() {

		return this.depth;
	}

	/**
	 * Returns the configured maximum allowed depth of this FD set.
	 *
	 * @return the maximum possible level depth
	 */
	public int getMaxDepth() {

		return this.maxDepth;
	}

	/**
	 * Adds a new functional dependency to this FD set.
	 * <p>
	 * The FD is placed into the corresponding level based on its number of attributes.
	 * </p>
	 *
	 * @param fd the {@link BitSet} representing the functional dependency
	 * @return {@code true} if the FD was newly added, {@code false} if it already existed or exceeds max depth
	 */
	public boolean add(BitSet fd) {
		int length = fd.cardinality();
		
		if ((this.maxDepth > 0) && (length > this.maxDepth))
			return false;
		
		this.depth = Math.max(this.depth, length);
		return this.fdLevels.get(length).add(fd);
	}
	/**
	 * Checks whether this FD set already contains a given functional dependency.
	 *
	 * @param fd the {@link BitSet} representing the functional dependency
	 * @return {@code true} if the FD is contained in the set, {@code false} otherwise
	 */
	public boolean contains(BitSet fd) {
		int length = fd.cardinality();
		
		if ((this.maxDepth > 0) && (length > this.maxDepth))
			return false;
		
		return this.fdLevels.get(length).contains(fd);
	}

	/**
	 * Trims this FD set to a specified maximum depth.
	 * <p/>
	 * All FD levels deeper than {@code newDepth} are removed.
	 *
	 * @param newDepth the new maximum depth to retain
	 */
	public void trim(int newDepth) {
		while (this.fdLevels.size() > (newDepth + 1)) // +1 because uccLevels contains level 0
			this.fdLevels.remove(this.fdLevels.size() - 1);
		
		this.depth = newDepth;
		this.maxDepth = newDepth;
	}

	/**
	 * Clears all functional dependencies stored in this FD set.
	 * <p>
	 * This operation resets all levels while preserving the original structure.
	 * </p>
	 */
	public void clear() {
		int numLevels = this.fdLevels.size();
		this.fdLevels = new ArrayList<ObjectOpenHashSet<BitSet>>(numLevels);
		for (int i = 0; i <= numLevels; i++)
			this.fdLevels.add(new ObjectOpenHashSet<BitSet>());
	}

	/**
	 * Returns the total number of functional dependencies contained in this FD set.
	 *
	 * @return the total count of FDs across all levels
	 */
	public int size() {
		int size = 0;
		for (ObjectOpenHashSet<BitSet> uccs : this.fdLevels)
			size += uccs.size();
		return size;
	}
}
