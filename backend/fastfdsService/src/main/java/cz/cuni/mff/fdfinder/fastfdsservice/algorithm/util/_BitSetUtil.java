package cz.cuni.mff.fdfinder.fastfdsservice.algorithm.util;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.BitSet;

public class _BitSetUtil {

    public static IntList convertToIntList(BitSet set) {
        IntList bits = new IntArrayList();
        int lastIndex = set.nextSetBit(0);
        while (lastIndex != -1) {
            bits.add(lastIndex);
            lastIndex = set.nextSetBit(lastIndex + 1);
        }
        return bits;
    }

    public static BitSet convertToBitSet(IntList list) {
        BitSet set = new BitSet(list.size());
        for (int l : list) {
            set.set(l);
        }
        return set;
    }
}
