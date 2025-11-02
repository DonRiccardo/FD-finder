package de.metanome.algorithms.depminer.depminer_helper.modules;

import java.util.*;
import de.metanome.algorithms.depminer.depminer_helper.modules.container.*;

/**
 * Represents a relation between tuples and their equivalence classes across multiple attributes.
 * Each tuple is linked to a set of (attributeID, equivalenceClassID) pairs.
 */
public class _TupleEquivalenceClassRelation {

	List<RelationshipPair> relationships = new LinkedList<RelationshipPair>();

	/**
	 * Adds a new (attributeID, equivalenceClassID) relationship.
	 */
	public void addNewRelationship(int attributeID, int equivalenceClassID) {

		this.addNewRelationship(new RelationshipPair(attributeID, equivalenceClassID));
	}

	/**
	 * Adds a new relationship pair to the list.
	 */
	public void addNewRelationship(RelationshipPair pair) {

		this.relationships.add(pair);
	}

	/**
	 * @return all stored relationships for this tuple.
	 */
	public List<RelationshipPair> getRelationships() {

		return this.relationships;
	}

	/**
	 * Merges relationships from another {@link _TupleEquivalenceClassRelation} instance into this one.
	 */
	public void mergeRelationshipsFrom(_TupleEquivalenceClassRelation otherRelations) {

		this.relationships.addAll(otherRelations.getRelationships());
	}

	/**
	 * Computes the intersection between this and another tuple's relationships.
	 * If they share at least one common (attribute, equivalenceClass) pair,
	 * adds the intersecting attributes as a new {@link  _AgreeSet} into the given concurrent map.
	 *
	 * @param other another {@link _TupleEquivalenceClassRelation}
	 * @param agreeSets concurrent map to collect {@link _AgreeSet}
	 */
	public void intersectWithAndAddToAgreeSetConcurrent(_TupleEquivalenceClassRelation other, Map<_AgreeSet, Object> agreeSets) {

		_AgreeSet set = new _AgreeSet();
		boolean intersected = false;
		for (RelationshipPair pair : this.relationships) {
			if (other.getRelationships().contains(pair)) {
				intersected = true;
				set.add(pair.getAttribute());
			}
		}

		if (intersected) {
			agreeSets.put(set, new Object());
		}
	}

	/**
	 * Computes the intersection between this and another tuple's relationships.
	 * If they share at least one (attribute, equivalenceClass) pair,
	 * adds the intersecting attributes as a new {@link _AgreeSet} to the provided set.
	 */
	public void intersectWithAndAddToAgreeSet(_TupleEquivalenceClassRelation other, Set<_AgreeSet> agreeSets) {

		_AgreeSet set = new _AgreeSet();
		boolean intersected = false;
		for (RelationshipPair pair : this.relationships) {
			if (other.getRelationships().contains(pair)) {
				intersected = true;
				set.add(pair.getAttribute());
			}
		}

		if (intersected) {
			agreeSets.add(set);
		}
	}

	@Override
	public int hashCode() {

		final int prime = 31;
		int result = 1;
		result = prime * result + ((relationships == null) ? 0 : relationships.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {

		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		_TupleEquivalenceClassRelation other = (_TupleEquivalenceClassRelation) obj;
		if (relationships == null) {
			if (other.relationships != null) {
				return false;
			}
		} else if (!relationships.equals(other.relationships)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {

		StringBuilder builder = new StringBuilder();
		for (RelationshipPair pair : this.relationships) {
			builder.append(pair.toString()).append(", ");
		}
		return builder.substring(0, builder.length() - 2);
	}

	/**
	 * Represents a pair (attributeID, equivalenceClassID)
	 * that defines which equivalence class a tuple belongs to for a given attribute.
	 */
	public static class RelationshipPair {

		private int[] relationship = new int[2];

		public RelationshipPair(int attribute, int ECIndex) {

			relationship[0] = attribute;
			relationship[1] = ECIndex;
		}

		/**
		 *
		 * @return {@link Integer} attribute
		 */
		public int getAttribute() {

			return this.relationship[0];
		}

		/**
		 *
		 * @return {@link Integer} index of equivalence class
		 */
		public int getIndex() {

			return this.relationship[1];
		}

		@Override
		public String toString() {

			return "(" + this.relationship[0] + ", " + this.relationship[1] + ")";
		}

		@Override
		public int hashCode() {

			final int prime = 31;
			int result = 1;
			result = prime * result + Arrays.hashCode(relationship);
			return result;
		}

		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			RelationshipPair other = (RelationshipPair) obj;
			if (!Arrays.equals(relationship, other.relationship)) {
				return false;
			}
			return true;
		}

	}

}
