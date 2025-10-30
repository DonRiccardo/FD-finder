package cz.cuni.mff.fdfinder.hyfdservice.algorithm.model;

import java.io.Serializable;
import java.util.BitSet;

public class FDTreeElementLhsPair implements Serializable {
	
	private final FDTreeElement element;
	private final BitSet lhs;
	
	public FDTreeElement getElement() {
		return this.element;
	}

	public BitSet getLhs() {
		return this.lhs;
	}

	public FDTreeElementLhsPair(FDTreeElement element, BitSet lhs) {
		this.element = element;
		this.lhs = lhs;
	}
}

