package de.metanome.algorithms.hyfd.structures;

/**
 * Represents an identifier for a cluster of values across multiple attributes.
 */
public class ClusterIdentifier {

	private final int[] cluster;

	/**
	 * Constructs a new {@code ClusterIdentifier} with the given cluster values.
	 *
	 * @param cluster an array of integers representing the cluster IDs for each attribute
	 */
	public ClusterIdentifier(final int[] cluster) {

		this.cluster = cluster;
	}

	/**
	 * Updates the cluster ID for the specified attribute index.
	 *
	 * @param index the index of the attribute to update
	 * @param clusterId the new cluster ID to assign to the attribute
	 */
	public void set(int index, int clusterId) {

		this.cluster[index] = clusterId;
	}

	/**
	 * Retrieves the cluster ID for the specified attribute index.
	 *
	 * @param index the index of the attribute
	 * @return the cluster ID assigned to the specified attribute
	 */
	public int get(int index) {

		return this.cluster[index];
	}

	/**
	 * Returns the underlying array of cluster IDs.
	 *
	 * @return an integer array representing the cluster identifiers
	 */
	public int[] getCluster() {

		return this.cluster;
	}

	/**
	 * Returns the number of attributes (i.e., the length of the cluster array).
	 *
	 * @return the number of elements in this cluster identifier
	 */
	public int size() {

		return this.cluster.length;
	}

	@Override
	public int hashCode() {
		int hash = 1;
		int index = this.size();
		while (index-- != 0)
			hash = 31 * hash + this.get(index);
		return hash;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (!(obj instanceof ClusterIdentifier))
			return false;
		final ClusterIdentifier other = (ClusterIdentifier) obj;
		
		int index = this.size();
		if (index != other.size())
			return false;
		
		final int[] cluster1 = this.getCluster();
		final int[] cluster2 = other.getCluster();
		
		while (index-- != 0)
			if (cluster1[index] != cluster2[index])
				return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("[");
		for (int i = 0; i < this.size(); i++) {
			builder.append(this.cluster[i]);
			if (i + 1 < this.size())
				builder.append(", ");
		}
		builder.append("]");
		return builder.toString();
	}
}
