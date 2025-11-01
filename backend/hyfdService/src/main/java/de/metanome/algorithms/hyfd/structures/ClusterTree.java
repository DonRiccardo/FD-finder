package de.metanome.algorithms.hyfd.structures;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.BitSet;


/**
 * Represents the root of a {@link ClusterTreeElement} hierarchy used for clustering records
 * based on combinations of attribute values
 */
public class ClusterTree {

	protected Int2ObjectOpenHashMap<ClusterTreeElement> children = new Int2ObjectOpenHashMap<ClusterTreeElement>();

	/**
	 * Adds a record into the cluster tree.
	 *
	 * @param compressedRecords the compressed dataset represented as an integer matrix,
	 *                          where each row corresponds to a record and each column to an attribute cluster ID
	 * @param lhs the set of attributes (as a {@link BitSet}) representing the left-hand side
	 *            of a potential functional dependency
	 * @param recordId the ID of the record to insert into the tree
	 * @param content the content or cluster identifier associated with this record,
	 *                typically corresponding to the right-hand side (RHS) attribute value
	 * @return {@code true} if the record was successfully added or a new branch was created,
	 *         {@code false} if the addition failed due to an inconsistent path or conflicting content
	 */
	public boolean add(int[][] compressedRecords, BitSet lhs, int recordId, int content) {
		int firstLhsAttr = lhs.nextSetBit(0);
		int firstCluster = compressedRecords[recordId][firstLhsAttr];
		if (firstCluster < 0)
			return true;
		
		ClusterTreeElement child = this.children.get(firstCluster);
		if (child == null) {
			child = new ClusterTreeElement(compressedRecords, lhs, lhs.nextSetBit(firstLhsAttr + 1), recordId, content);
			this.children.put(firstCluster, child);
			return true;
		}
		
		return child.add(compressedRecords, lhs, lhs.nextSetBit(firstLhsAttr + 1), recordId, content);
	}
}
