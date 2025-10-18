/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package cz.cuni.mff.fdfinder.depminerservice.algorithm.service;

import cz.cuni.mff.fdfinder.depminerservice.algorithm.model._CMAX_SET;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.*;

/**
 *
 * @author pavel.koupil
 */
public class _LeftHandSideGenerator {

	public _LeftHandSideGenerator() {
	}

	/**
	 * Computes the LHS
	 *
	 * @param maximalSets The set of the complements of maximal sets (see Phase 2 for further information)
	 * @param nrOfAttributes The number attributes in the whole relation
	 * @return {@code Int2ObjectMap<List<OpenBitSet>>} (key: dependent attribute, value: set of all lefthand sides)
	 */
	public Int2ObjectMap<List<BitSet>> execute(List<_CMAX_SET> maximalSets, int nrOfAttributes, int maxLhs) {

		Int2ObjectMap<List<BitSet>> lhs = new Int2ObjectOpenHashMap<>();

		/* 1: for all attributes A in R do */
		for (int attribute = 0; attribute < nrOfAttributes; attribute++) {
//			System.out.println("Attribute: " + attribute);
			/* 2: i:=1 */
			 int i = 1;

			/* 3: Li:={B | B in X, X in cmax(dep(r),A)} */
			Set<BitSet> Li = new HashSet<>();
			_CMAX_SET correctSet = this.generateFirstLevelAndFindCorrectSet(maximalSets, attribute, Li);
//			System.out.println("Attribute: " + attribute + " after generate first level");

			List<List<BitSet>> lhs_a = new LinkedList<>();

			/* 4: while Li != ø do */
//			int counter = 0;
			while (!Li.isEmpty()) {
//				++counter;
//				System.out.println("not empty " + counter + " :: " + Li.size());
				/*
                 * 5: LHSi[A]:={l in Li | l intersect X != ø, for all X in cmax(dep(r),A)}
				 */
				List<BitSet> lhs_i = findLHS(Li, correctSet);

				/* 6: Li:=Li/LHSi[A] */
				Li.removeAll(lhs_i);

				/*
                 * 7: Li+1:={l' | |l'|=i+1 and for all l subset l' | |l|=i, l in Li}
				 */
 /*
				 * The generation of the next level is, as mentioned in the paper, done with the Apriori gen-function from the
				 * following paper: "Fast algorithms for mining association rules in large databases." - Rakesh Agrawal,
				 * Ramakrishnan Srikant
				 */
				if (i >= maxLhs) {

					Li.clear();
				}
				else {

					Li = this.generateNextLevel(Li);
				}


				/* 8: i:=i+1 */
				 i++;
				lhs_a.add(lhs_i);
			}

			/* 9: lhs(dep(r),A):= union LHSi[A] */
			if (!lhs.containsKey(attribute)) {
				lhs.put(attribute, new LinkedList<BitSet>());
//				System.out.println("LHS_SIZE: " + lhs.size());
			}
			for (List<BitSet> lhs_ia : lhs_a) {
				lhs.get(attribute).addAll(lhs_ia);
			}
		}

//		System.out.println("RETURNING LHS_SIZE: " + lhs.size());
		return lhs;
	}

	private List<BitSet> findLHS(Set<BitSet> Li, _CMAX_SET correctSet) {

		List<BitSet> lhs_i = new LinkedList<>();
		for (BitSet l : Li) {
			boolean isLHS = true;
			for (BitSet x : correctSet.getCombinations()) {
				if (!l.intersects(x)) {
					isLHS = false;
					break;
				}
			}
			if (isLHS) {
				lhs_i.add(l);
			}
		}
		return lhs_i;
	}

	private _CMAX_SET generateFirstLevelAndFindCorrectSet(List<_CMAX_SET> maximalSets, int attribute, Set<BitSet> Li) {

		_CMAX_SET correctSet = null;
		for (_CMAX_SET set : maximalSets) {
			if (!(set.getAttribute() == attribute)) {
				continue;
			}
			correctSet = set;
			for (BitSet list : correctSet.getCombinations()) {

				BitSet combination;
				int lastIndex = list.nextSetBit(0);
				while (lastIndex != -1) {
					combination = new BitSet();
					combination.set(lastIndex);
					Li.add(combination);
					lastIndex = list.nextSetBit(lastIndex + 1);
				}
			}
			break;
		}
		return correctSet;
	}

	private Set<BitSet> generateNextLevel(Set<BitSet> li) {
                //System.out.println("LHS--GEN-li " + li);
		// Join-Step
		List<BitSet> Ck = new LinkedList<>();
		for (BitSet p : li) {
			for (BitSet q : li) {
				if (!this.checkJoinCondition(p, q)) {
					continue;
				}
				BitSet candidate = new BitSet();
				candidate.or(p);
				candidate.or(q);
				Ck.add(candidate);
			}
		}

		// Pruning-Step
		Set<BitSet> result = new HashSet<>();
		for (BitSet c : Ck) {
			boolean prune = false;
			int lastIndex = c.nextSetBit(0);
			while (lastIndex != -1) {
				c.flip(lastIndex);
				if (!li.contains(c)) {
					prune = true;
					break;
				}
				c.flip(lastIndex);
				lastIndex = c.nextSetBit(lastIndex + 1);
			}

			if (!prune) {
				result.add(c);
			}
		}
                //System.out.println("LHS--GEN--Next-level "+result);
		return result;

	}

	private boolean checkJoinCondition(BitSet p, BitSet q) {
                //System.out.println("LHS--GEN-Check "+ p + q);
                //System.out.println("LHS--GEN-Check-prevSetBit "+ p.previousSetBit(p.length()) + q.previousSetBit(q.length()));
		if (p.previousSetBit(p.length()) >= q.previousSetBit(q.length())) {
			return false;
		}
		for (int i = 0; i < p.previousSetBit(p.length()); i++) {
			if (p.get(i) != q.get(i)) {
				return false;
			}
		}
		return true;
	}

}
