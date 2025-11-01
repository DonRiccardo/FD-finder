package de.metanome.algorithms.hyfd.structures;

import java.io.Serializable;

/**
 * Represents an immutable pair of integers.
 * Useful for storing pairs of record indices, attribute indices, or any two related integers.
 */
public class IntegerPair implements Serializable {

	private final int a;
	private final int b;

	/**
	 * Constructs an {@code IntegerPair} with the specified values.
	 *
	 * @param a the first integer
	 * @param b the second integer
	 */
	public IntegerPair(final int a, final int b) {
		this.a = a;
		this.b = b;
	}

	/**
	 * Returns the first integer in the pair.
	 *
	 * @return the first integer
	 */
	public int a() {

		return this.a;
	}

	/**
	 * Returns the second integer in the pair.
	 *
	 * @return the second integer
	 */
	public int b() {

		return this.b;
	}
}
