package de.metanome.algorithms.depminer.depminer_helper.modules.container;

import de.metanome.algorithms.depminer.depminer_helper.util._BitSetUtil;

import java.io.Serializable;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;

/**
 * Represents a MAX set for a given attribute.
 */
public class _MAX_SET extends _CMAX_SET implements Serializable{

	private boolean finalized;

	/**
	 * Constructs a MAX set for the given attribute.
	 */
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

	/**
	 * Removes all non-maximal combinations from columnCombinations.
	 * Iterates through all current sets and retains only those that are
	 * not proper subsets of any other set.
	 */
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

	/**
	 * Checks whether the first {@link BitSet} is a superset of the second.
	 *
	 * @param set {@link BitSet} the candidate superset
	 * @param set2 {@link BitSet} the candidate subset
	 * @return {@code true} if set is a superset of set2, {@code false} otherwise
	 */
	private boolean checkIfSetIsSuperSetOf(BitSet set, BitSet set2) {
		BitSet setCopy = (BitSet)set.clone();
		setCopy.and(set2);
		return setCopy.equals(set2);
	}

}
