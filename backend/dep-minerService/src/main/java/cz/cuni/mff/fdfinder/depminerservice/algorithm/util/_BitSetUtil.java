/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package cz.cuni.mff.fdfinder.depminerservice.algorithm.util;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import org.apache.lucene.util.OpenBitSet;

import java.util.BitSet;

/**
 *
 * @author pavel.koupil
 */
public class _BitSetUtil {

	public static LongList convertToLongList(BitSet set) {
		LongList bits = new LongArrayList();
		int lastIndex = set.nextSetBit(0);
		while (lastIndex != -1) {
			bits.add(lastIndex);
			lastIndex = set.nextSetBit(lastIndex + 1);
		}
		return bits;
	}

	public static IntList convertToIntList(BitSet set) {
		IntList bits = new IntArrayList();
		int lastIndex = set.nextSetBit(0);
		while (lastIndex != -1) {
			bits.add(lastIndex);
			lastIndex = set.nextSetBit(lastIndex + 1);
		}
		return bits;
	}

	public static OpenBitSet convertToBitSet(LongList list) {
		OpenBitSet set = new OpenBitSet(list.size());
		for (long l : list) {
			set.fastSet(l);
		}
		return set;
	}

}
