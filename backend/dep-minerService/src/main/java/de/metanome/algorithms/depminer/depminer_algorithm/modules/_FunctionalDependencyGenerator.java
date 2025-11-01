package de.metanome.algorithms.depminer.depminer_algorithm.modules;

import cz.cuni.mff.fdfinder.depminerservice.algorithm.model._Input;
import de.metanome.algorithms.depminer.depminer_helper.modules.container._FunctionalDependencyGroup;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;

/**
 * Generates functional dependencies (FDs) from previously discovered LHS candidates.
 */
public class _FunctionalDependencyGenerator {

	private _Input fdrr;
	private String relationName;
	private List<String> columns;
	private Int2ObjectMap<List<BitSet>> lhss;

	private Exception exception = null;

	private List<_FunctionalDependencyGroup> result;

	/**
	 * Creates a new functional dependency generator.
	 *
	 * @param fdrr {@link _Input} and result handler object
	 * @param relationName {@link String} name of the relation
	 * @param columnIdentifer list of {@link String} column (attribute) names
	 * @param lhss map of attributes to their corresponding LHS candidates
	 */
	public _FunctionalDependencyGenerator(_Input fdrr, String relationName, List<String> columnIdentifer, Int2ObjectMap<List<BitSet>> lhss) {
		this.fdrr = fdrr;
		this.relationName = relationName;
		this.columns = columnIdentifer;
		this.lhss = lhss;
	}

	/**
	 * Generates all valid functional dependencies of the form X → A.
	 *
	 * <p>For each dependent attribute A, and for each valid LHS bitset X (where A ∉ X),
	 * this method creates a {@link _FunctionalDependencyGroup}, builds a dependency,
	 * and passes it to the input receiver.</p>
	 *
	 * @return list of generated {@link _FunctionalDependencyGroup} objects
	 * @throws Exception if any exception was thrown during dependency construction
	 */
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
			}
		}

		if (this.exception != null) {
			throw this.exception;
		}

		return this.result;
	}

}
