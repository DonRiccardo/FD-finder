package de.metanome.algorithms.depminer.depminer_helper.util;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import org.apache.lucene.util.OpenBitSet;

import java.util.BitSet;

/**
 * Utility class for converting between different bit set representations.
 */
public class _BitSetUtil {

	/**
	 * Converts a {@link BitSet} into a {@link LongList} containing
	 * the indices of all bits that are set to 1.
	 *
	 * @param set the {@link BitSet} to convert
	 * @return a {@link LongList} of indices of all set bits
	 */
	public static LongList convertToLongList(BitSet set) {
		LongList bits = new LongArrayList();
		int lastIndex = set.nextSetBit(0);
		while (lastIndex != -1) {
			bits.add(lastIndex);
			lastIndex = set.nextSetBit(lastIndex + 1);
		}
		return bits;
	}

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
	 * Converts a {@link LongList} of indices into an {@link OpenBitSet},
	 * where each index in the list corresponds to a bit that should be set to 1.
	 *
	 * @param list the list of indices to set in the {@link OpenBitSet}
	 * @return a new {@link OpenBitSet} with bits set at the given indices
	 */
	public static OpenBitSet convertToBitSet(LongList list) {
		OpenBitSet set = new OpenBitSet(list.size());
		for (long l : list) {
			set.fastSet(l);
		}
		return set;
	}

}
