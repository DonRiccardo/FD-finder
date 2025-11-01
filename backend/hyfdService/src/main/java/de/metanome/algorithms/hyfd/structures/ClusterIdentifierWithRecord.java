package de.metanome.algorithms.hyfd.structures;

/**
 * Represents a {@link ClusterIdentifier} that additionally stores the record ID
 * associated with the cluster.
 */
public class ClusterIdentifierWithRecord extends ClusterIdentifier {

	private final int record;

	/**
	 * Constructs a new {@code ClusterIdentifierWithRecord} with the specified cluster and record ID.
	 *
	 * @param cluster an array of integers representing the cluster identifier
	 *                (e.g., attribute cluster values for a given record)
	 * @param record the record ID corresponding to this cluster
	 */
	public ClusterIdentifierWithRecord(final int[] cluster, final int record) {
		super(cluster);
		this.record = record;
	}

	/**
	 * Returns the record ID associated with this cluster identifier.
	 *
	 * @return the record ID linked to this cluster
	 */
	public int getRecord() {

		return this.record;
	}
	
}
