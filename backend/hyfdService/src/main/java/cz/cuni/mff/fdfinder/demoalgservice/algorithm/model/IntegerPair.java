package cz.cuni.mff.fdfinder.demoalgservice.algorithm.model;

import java.io.Serializable;

public class IntegerPair implements Serializable {

	private final int a;
	private final int b;
	
	public IntegerPair(final int a, final int b) {
		this.a = a;
		this.b = b;
	}
	
	public int a() {
		return this.a;
	}
	
	public int b() {
		return this.b;
	}
}
