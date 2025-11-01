package de.metanome.algorithms.depminer.depminer_helper.modules.container;

import de.metanome.algorithms.depminer.depminer_helper.util._BitSetUtil;

import java.io.Serializable;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;

/**
 * Represents a CMAX set for a given attribute.
 */
public class _CMAX_SET implements Serializable{

	protected int attribute;
	protected List<BitSet> columnCombinations;
	private boolean finalized;

	/**
	 * Constructs a CMAX set for a specific attribute.
	 */
	public _CMAX_SET(int attribute) {

		this.attribute = attribute;
		this.columnCombinations = new LinkedList<>();
		this.finalized = false;
	}

	/**
	 * Adds a new column combination to this {@link _CMAX_SET}.
	 *
	 * @param combination {@link BitSet} representing a combination of columns
	 */
	public void addCombination(BitSet combination) {

		this.columnCombinations.add(combination);
	}

	/**
	 * @return all column combinations stored in this {@link _CMAX_SET}.
	 */
	public List<BitSet> getCombinations() {

		return this.columnCombinations;
	}

	/**
	 * @return {@link Integer} attribute associated with this {@link _CMAX_SET}.
	 */
	public int getAttribute() {

		return this.attribute;
	}

	@Override
	public String toString() {

		String s = "cmax(" + this.attribute + ": ";
		for (BitSet set : this.columnCombinations) {
			s += _BitSetUtil.convertToLongList(set);
		}
		return s + ")";
	}

	/**
	 * Marks this {@link _CMAX_SET} as finalized. After this, the set should not be modified.
	 */
	public void finalize_RENAME_THIS() {

		this.finalized = true;
	}

	@Override
	public int hashCode() {

		final int prime = 31;
		int result = 1;
		result = prime * result + attribute;
		result = prime * result + ((columnCombinations == null) ? 0 : columnCombinations.hashCode());
		result = prime * result + (finalized ? 1231 : 1237);
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
		_CMAX_SET other = (_CMAX_SET) obj;
		if (attribute != other.attribute) {
			return false;
		}
		if (columnCombinations == null) {
			if (other.columnCombinations != null) {
				return false;
			}
		} else if (!columnCombinations.equals(other.columnCombinations)) {
			return false;
		}
		if (finalized != other.finalized) {
			return false;
		}
		return true;
	}

}
