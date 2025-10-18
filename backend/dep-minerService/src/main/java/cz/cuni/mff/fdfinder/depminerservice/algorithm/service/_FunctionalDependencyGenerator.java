/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package cz.cuni.mff.fdfinder.depminerservice.algorithm.service;

import cz.cuni.mff.fdfinder.depminerservice.algorithm.model._Input;
import cz.cuni.mff.fdfinder.depminerservice.algorithm.model._FunctionalDependencyGroup;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author pavel.koupil
 */
public class _FunctionalDependencyGenerator {

	private _Input fdrr;
	private String relationName;
	private List<String> columns;
	private Int2ObjectMap<List<BitSet>> lhss;

	private Exception exception = null;

	private List<_FunctionalDependencyGroup> result;

	public _FunctionalDependencyGenerator(_Input fdrr, String relationName, List<String> columnIdentifer, Int2ObjectMap<List<BitSet>> lhss) {
		this.fdrr = fdrr;
		this.relationName = relationName;
		this.columns = columnIdentifer;
		this.lhss = lhss;
	}

	public List<_FunctionalDependencyGroup> execute() throws Exception {

		this.result = new LinkedList<>();
		for (int attribute : this.lhss.keySet()) {
			for (BitSet lhs : this.lhss.get(attribute)) {
				if (lhs.get(attribute)) {
					continue;
				}
				IntList bits = new IntArrayList();
				int lastIndex = lhs.nextSetBit(0);
				while (lastIndex != -1) {
					bits.add(lastIndex);
					lastIndex = lhs.nextSetBit(lastIndex + 1);
				}

				_FunctionalDependencyGroup fdg = new _FunctionalDependencyGroup(attribute, bits);
				this.fdrr.receiveResult((fdg.buildDependency(this.relationName, this.columns)));
				//this.result.add(fdg);
			}
		}

		if (this.exception != null) {
			throw this.exception;
		}

		return this.result;
	}

}
