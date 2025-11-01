package de.metanome.algorithms.hyfd;

import de.metanome.algorithms.hyfd.structures.FDList;
import de.metanome.algorithms.hyfd.structures.FDSet;
import de.metanome.algorithms.hyfd.structures.FDTree;
import de.metanome.algorithms.hyfd.utils.Logger;

import java.util.BitSet;
import java.util.List;

/**
 * The {@link Inductor} class is responsible for inducing functional dependency (FD) candidates
 * based on the {@link FDList} of non-functional dependencies (non-FDs) and updating the
 * {@link FDTree} representing the positive cover.
 */
public class Inductor {

	private FDSet negCover;
	private FDTree posCover;
	private MemoryGuardian memoryGuardian;

	/**
	 * Constructs an {@link Inductor} with the given positive and negative covers
	 * and a {@link MemoryGuardian}.
	 *
	 * @param negCover the {@link FDSet} storing non-FDs
	 * @param posCover the {@link FDTree} storing FDs
	 * @param memoryGuardian the {@link MemoryGuardian} instance for memory management
	 */
	public Inductor(FDSet negCover, FDTree posCover, MemoryGuardian memoryGuardian) {
		this.negCover = negCover;
		this.posCover = posCover;
		this.memoryGuardian = memoryGuardian;
	}

	/**
	 * Updates the positive cover ({@link #posCover}) by specializing it based on
	 * the given {@link FDList} of non-functional dependencies.
	 *
	 * <p>Iterates over the non-FDs in reverse level order, generating specialized
	 * FD candidates and removing invalidated FDs from the positive cover.</p>
	 *
	 * @param nonFds the {@link FDList} containing non-functional dependencies
	 */
	public void updatePositiveCover(FDList nonFds) {

            // THE SORTING IS NOT NEEDED AS THE UCCSet SORTS THE NONUCCS BY LEVEL ALREADY

            Logger.getInstance().writeln("Inducing FD candidates ...");
            for (int i = nonFds.getFdLevels().size() - 1; i >= 0; i--) {

                // ?? what is this IF doing ??
                if (i >= nonFds.getFdLevels().size()) // If this level has been trimmed during iteration
                    continue;

                List<BitSet> nonFdLevel = nonFds.getFdLevels().get(i);
                for (BitSet lhs : nonFdLevel) {

                    BitSet fullRhs = (BitSet) lhs.clone();
                    fullRhs.flip(0, this.posCover.getNumAttributes());

                    for (int rhs = fullRhs.nextSetBit(0); rhs >= 0; rhs = fullRhs.nextSetBit(rhs + 1))
                        this.specializePositiveCover(lhs, rhs, nonFds);
                }
                nonFdLevel.clear();
            }
	}

	/**
	 * Specializes the positive cover by removing the given FD ({@code lhs -> rhs})
	 * and generating all minimal specializations that are not already present in
	 * the positive cover or its generalizations.
	 *
	 * @param lhs the left-hand side of the FD to specialize
	 * @param rhs the right-hand side attribute of the FD
	 * @param nonFds the {@link FDList} of non-FDs to consider during memory management
	 * @return the number of new FDs added to the positive cover
	 */
	protected int specializePositiveCover(BitSet lhs, int rhs, FDList nonFds) {
		int numAttributes = this.posCover.getChildren().length;
		int newFDs = 0;
		List<BitSet> specLhss;
		
		if (!(specLhss = this.posCover.getFdAndGeneralizations(lhs, rhs)).isEmpty()) { // TODO: May be "while" instead of "if"?
			for (BitSet specLhs : specLhss) {
				this.posCover.removeFunctionalDependency(specLhs, rhs);
				
				if ((this.posCover.getMaxDepth() > 0) && (specLhs.cardinality() >= this.posCover.getMaxDepth()))
					continue;
				
				for (int attr = numAttributes - 1; attr >= 0; attr--) { // TODO: Is iterating backwards a good or bad idea?
					if (!lhs.get(attr) && (attr != rhs)) {
						specLhs.set(attr);
						if (!this.posCover.containsFdOrGeneralization(specLhs, rhs)) {
							this.posCover.addFunctionalDependency(specLhs, rhs);
							newFDs++;
							
							// If dynamic memory management is enabled, frequently check the memory consumption and trim the positive cover if it does not fit anymore
							this.memoryGuardian.memoryChanged(1);
							this.memoryGuardian.match(this.negCover, this.posCover, nonFds);
						}
						specLhs.clear(attr);
					}
				}
			}
		}
		return newFDs;
	}
}
