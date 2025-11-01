package de.metanome.algorithms.fastfds.fastfds_helper.util;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.BitSet;

/**
 * Utility class for converting between different bit set representations.
 */
public class _BitSetUtil {

    /**
     * Converts a {@link BitSet} into an {@link IntList} containing
     * the indices of all bits that are set to 1.
     *
     * This version uses integers and is suitable when all bit indices fit into the int range.
     *
     * @param set the {@link BitSet} to convert
     * @return an {@link IntList} of indices of all set bits
     */
    public static IntList convertToIntList(BitSet set) {
        IntList bits = new IntArrayList();
        int lastIndex = set.nextSetBit(0);
        while (lastIndex != -1) {
            bits.add(lastIndex);
            lastIndex = set.nextSetBit(lastIndex + 1);
        }
        return bits;
    }

    /**
     * Converts a {@link IntList} of indices into an {@link BitSet},
     * where each index in the list corresponds to a bit that should be set to 1.
     *
     * @param list the list of indices to set in the {@link BitSet}
     * @return a new {@link BitSet} with bits set at the given indices
     */
    public static BitSet convertToBitSet(IntList list) {
        BitSet set = new BitSet(list.size());
        for (int l : list) {
            set.set(l);
        }
        return set;
    }
}
