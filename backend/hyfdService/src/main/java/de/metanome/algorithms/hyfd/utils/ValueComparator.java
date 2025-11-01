package de.metanome.algorithms.hyfd.utils;

/**
 * Utility class for comparing values with optional null equality semantics.
 */
public class ValueComparator {

	private boolean isNullEqualNull;

	/**
	 * Constructs a {@code ValueComparator}.
	 *
	 * @param isNullEqualNull {@code true} if {@code null} values should be considered equal, {@code false} otherwise
	 */
	public ValueComparator(boolean isNullEqualNull) {

		this.isNullEqualNull = isNullEqualNull;
	}

	/**
	 * Returns whether this comparator considers {@code null} values equal.
	 *
	 * @return {@code true} if {@code null} values are considered equal, {@code false} otherwise
	 */
	public boolean isNullEqualNull() {

		return this.isNullEqualNull;
	}

	/**
	 * Compares two {@link Object} values for equality, taking into account the
	 * {@code isNullEqualNull} setting.
	 *
	 * @param val1 the first value
	 * @param val2 the second value
	 * @return {@code true} if the values are considered equal, {@code false} otherwise
	 */
	public boolean isEqual(Object val1, Object val2) {
		if ((val1 == null) && (val2 == null))
			return this.isNullEqualNull;
		
		return (val1 != null) && val1.equals(val2);
	}

	/**
	 * Compares two {@code int} values for equality. Only non-negative values
	 * are considered valid for equality.
	 *
	 * @param val1 the first value
	 * @param val2 the second value
	 * @return {@code true} if both values are non-negative and equal, {@code false} otherwise
	 */
	public boolean isEqual(int val1, int val2) {

		return (val1 >= 0) && (val2 >= 0) && (val1 == val2);
	}

	/**
	 * Determines whether two {@code int} values are different.
	 * Values less than zero are treated as invalid and considered different.
	 *
	 * @param val1 the first value
	 * @param val2 the second value
	 * @return {@code true} if the values are different, {@code false} otherwise
	 */
	public boolean isDifferent(int val1, int val2) {

		return (val1 < 0) || (val2 < 0) || (val1 != val2);
	}
}
