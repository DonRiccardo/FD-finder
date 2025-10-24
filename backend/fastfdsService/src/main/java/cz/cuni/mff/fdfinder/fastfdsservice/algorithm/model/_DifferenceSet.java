package cz.cuni.mff.fdfinder.fastfdsservice.algorithm.model;

import cz.cuni.mff.fdfinder.fastfdsservice.algorithm.model._AgreeSet;

import java.util.BitSet;

public class _DifferenceSet extends _AgreeSet {

    public _DifferenceSet(BitSet obs) {

        this.attributes = obs;
    }

    public _DifferenceSet() {

        this(new BitSet());
    }
    
    @Override
    public String toString() {

        //return "diff(" + _BitSetUtil.convertToIntList(this.attributes).toString() + ")";
        return "diff("+this.attributes+")";
    }
}
