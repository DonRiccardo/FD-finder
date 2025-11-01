package de.metanome.algorithms.hyfd.structures;

import java.io.Serializable;
import java.util.BitSet;

/**
 * Represents a pairing of an {@link FDTreeElement} with its corresponding left-hand side (LHS)
 * as a {@link BitSet}.
 */
public class FDTreeElementLhsPair implements Serializable {
	
	private final FDTreeElement element;
	private final BitSet lhs;

	/**
	 * Returns the FD tree element.
	 *
	 * @return the FD tree element
	 */
	public FDTreeElement getElement() {

		return this.element;
	}

	/**
	 * Returns the BitSet representing the LHS.
	 *
	 * @return the LHS BitSet
	 */
	public BitSet getLhs() {

		return this.lhs;
	}

	/**
	 * Constructs a new {@code FDTreeElementLhsPair} with the specified element and LHS.
	 *
	 * @param element the FD tree element
	 * @param lhs the BitSet representing the LHS
	 */
	public FDTreeElementLhsPair(FDTreeElement element, BitSet lhs) {
		this.element = element;
		this.lhs = lhs;
	}
}

