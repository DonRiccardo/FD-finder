package cz.cuni.mff.fdfinder.hyfdservice.algorithm.services;

import cz.cuni.mff.fdfinder.hyfdservice.algorithm.model.FDList;
import cz.cuni.mff.fdfinder.hyfdservice.algorithm.model.FDSet;
import cz.cuni.mff.fdfinder.hyfdservice.algorithm.model.FDTree;
import cz.cuni.mff.fdfinder.hyfdservice.algorithm.utils.Logger;

import java.util.BitSet;
import java.util.List;

public class Inductor {

	private FDSet negCover;
	private FDTree posCover;
	private MemoryGuardian memoryGuardian;

	public Inductor(FDSet negCover, FDTree posCover, MemoryGuardian memoryGuardian) {
		this.negCover = negCover;
		this.posCover = posCover;
		this.memoryGuardian = memoryGuardian;
	}

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
