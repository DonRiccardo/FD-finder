/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package cz.cuni.mff.fdfinder.depminerservice.algorithm.model;

import cz.cuni.mff.fdfinder.depminerservice.algorithm.util._BitSetUtil;

import java.io.Serializable;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author pavel.koupil
 */
public class _MAX_SET extends _CMAX_SET implements Serializable{

	private boolean finalized;

	public _MAX_SET(int attribute) {
		super(attribute);
		this.finalized = false;
	}

	@Override
	public String toString() {

		String s = "max(" + this.attribute + ": ";
		for (BitSet set : this.columnCombinations) {
			s += _BitSetUtil.convertToLongList(set);
		}
		return s + ")";
	}

	@Override
	public void finalize_RENAME_THIS() {

		if (!this.finalized) {
			this.checkContentForOnlySuperSets();
		}
		this.finalized = true;

	}

	private void checkContentForOnlySuperSets() {
                //System.out.println("CHECKING--MAX "+this.attribute);
		List<BitSet> superSets = new LinkedList<BitSet>();
		List<BitSet> toDelete = new LinkedList<BitSet>();
		boolean toAdd = true;

		for (BitSet set : this.columnCombinations) {
			for (BitSet superSet : superSets) {
				if (this.checkIfSetIsSuperSetOf(set, superSet)) {
					toDelete.add(superSet);
                                        //System.out.println("CHECKING--MAX--DEL "+superSet);
				}
				if (toAdd) {
					toAdd = !this.checkIfSetIsSuperSetOf(superSet, set);
				}
			}
			superSets.removeAll(toDelete);
			if (toAdd) {
				superSets.add(set);
                                //System.out.println("CHECKING--MAX--ADD "+set);
			} else {
				toAdd = true;
			}
			toDelete.clear();
		}

		this.columnCombinations = superSets;
	}

	private boolean checkIfSetIsSuperSetOf(BitSet set, BitSet set2) {
		BitSet setCopy = (BitSet)set.clone();
		setCopy.and(set2);
		return setCopy.equals(set2);
	}

}
