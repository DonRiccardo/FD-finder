package cz.cuni.mff.fdfinder.taneservice.algorithm.model;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.BitSet;
import java.util.List;

/**
 * Create FD from {@link Integer} values representing columns.
 */
public class _FunctionalDependencyGroup {

	private int attribute = Integer.MIN_VALUE;
	private BitSet values = new BitSet();

	public _FunctionalDependencyGroup(int attributeID, BitSet values) {

		this.attribute = attributeID;
		this.values.or(values);
	}

	/**
	 *
	 * @return {@link Integer} of attribute on RHS
	 */
	public int getAttributeID() {

		return this.attribute;
	}

	/**
	 *
	 * @return {@link BitSet} of attributes on LHS
	 */
	public BitSet getValues() {

		BitSet returnValue = new BitSet();
		returnValue.or(this.values);
		return returnValue;

	}

	@Override
	public String toString() {

		return this.values + " --> " + this.attribute;
	}

	@Override
	public int hashCode() {

		final int prime = 31;
		int result = 1;
		result = prime * result + attribute;
		result = prime * result + ((values == null) ? 0 : values.hashCode());
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
		_FunctionalDependencyGroup other = (_FunctionalDependencyGroup) obj;
		if (attribute != other.attribute) {
			return false;
		}
		if (values == null) {
			if (other.values != null) {
				return false;
			}
		} else if (!values.equals(other.values)) {
			return false;
		}
		return true;
	}

	/**
	 * Create a {@link _FunctionalDependency} from set data.
	 * @param tableIdentifier {@link String} name of the table (dataset)
	 * @param columnNames List ({@link String}) of column names
	 * @return builded {@link _FunctionalDependency}
	 */
	public _FunctionalDependency buildDependency(String tableIdentifier, List<String> columnNames) {

		_FunctionalDependency._ColumnIdentifier[] combination = new _FunctionalDependency._ColumnIdentifier[this.values.cardinality()];
		int j = 0;
		for (int i = this.values.nextSetBit(0); i >= 0; i = this.values.nextSetBit(i + 1)) {
			combination[j] = new _FunctionalDependency._ColumnIdentifier(tableIdentifier, columnNames.get(i));
			j++;
		}
		_FunctionalDependency._ColumnCombination cc = new _FunctionalDependency._ColumnCombination(combination);
		_FunctionalDependency._ColumnIdentifier ci = new _FunctionalDependency._ColumnIdentifier(tableIdentifier, columnNames.get(this.attribute));
		_FunctionalDependency fd = new _FunctionalDependency(cc, ci);
		return fd;
	}

}
