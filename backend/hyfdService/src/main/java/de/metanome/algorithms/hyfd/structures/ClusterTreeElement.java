package de.metanome.algorithms.hyfd.structures;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.BitSet;


/**
 * Represents a recursive tree structure used for organizing and clustering
 * records based on attribute combinations.
 */
public class ClusterTreeElement {

	protected Int2ObjectOpenHashMap<ClusterTreeElement> children = new Int2ObjectOpenHashMap<ClusterTreeElement>();
	protected int content = 0;

	/**
	 * Constructs a new {@code ClusterTreeElement} recursively from compressed records.
	 * <p>
	 * This constructor initializes a tree node and, if further attributes are available,
	 * recursively creates child nodes corresponding to subsequent attributes in {@code lhs}.
	 * </p>
	 *
	 * @param compressedRecords the compressed dataset represented as an integer matrix,
	 *                          where each row corresponds to a record and each column to an attribute cluster ID
	 * @param lhs the set of attributes (as a {@link BitSet}) representing the current path of the tree
	 * @param nextLhsAttr the index of the next attribute in {@code lhs} to process,
	 *                    or {@code -1} if this is the last attribute
	 * @param recordId the ID of the record being inserted
	 * @param content the content or identifier to be stored at the leaf node
	 */
	public ClusterTreeElement(int[][] compressedRecords, BitSet lhs, int nextLhsAttr, int recordId, int content) {
		if (nextLhsAttr < 0) {
			this.content = content;
		}
		else {
			int nextCluster = compressedRecords[recordId][nextLhsAttr];
			if (nextCluster < 0)
				return;
			
			ClusterTreeElement child = new ClusterTreeElement(compressedRecords, lhs, lhs.nextSetBit(nextLhsAttr + 1), recordId, content);
			this.children.put(nextCluster, child);
		}
	}

	/**
	 * Adds a record to the cluster tree, creating new child nodes as necessary.
	 * <p>
	 * If the current attribute position ({@code nextLhsAttr}) is negative,
	 * this node is treated as a leaf, and its content is validated against the provided one.
	 * Otherwise, the method continues recursively along the path defined by {@code lhs}.
	 * </p>
	 *
	 * @param compressedRecords the compressed dataset represented as an integer matrix,
	 *                          where each row corresponds to a record and each column to an attribute cluster ID
	 * @param lhs the set of attributes (as a {@link BitSet}) representing the current path of the tree
	 * @param nextLhsAttr the index of the next attribute in {@code lhs} to process,
	 *                    or {@code -1} if this is the last attribute
	 * @param recordId the ID of the record being inserted
	 * @param content the content or identifier to be stored at the leaf node
	 * @return {@code true} if the insertion succeeded or created a new branch,
	 *         {@code false} if the insertion encountered a content mismatch
	 */
	public boolean add(int[][] compressedRecords, BitSet lhs, int nextLhsAttr, int recordId, int content) {
		if (nextLhsAttr < 0)
			return this.content != -1 && this.content == content;
		
		int nextCluster = compressedRecords[recordId][nextLhsAttr];
		if (nextCluster < 0)
			return true;
		
		ClusterTreeElement child = this.children.get(nextCluster);
		if (child == null) {
			child = new ClusterTreeElement(compressedRecords, lhs, lhs.nextSetBit(nextLhsAttr + 1), recordId, content);
			this.children.put(nextCluster, child);
			return true;
		}
		
		return child.add(compressedRecords, lhs, lhs.nextSetBit(nextLhsAttr + 1), recordId, content);
	}
}
