package de.metanome.algorithms.hyfd.structures;

import cz.cuni.mff.fdfinder.hyfdservice.algorithm.model._Input;
import de.metanome.algorithms.depminer.depminer_helper.modules.container._FunctionalDependency;
import de.metanome.algorithms.depminer.depminer_helper.modules.container._FunctionalDependency._ColumnCombination;
import de.metanome.algorithms.depminer.depminer_helper.modules.container._FunctionalDependency._ColumnIdentifier;
import de.uni_potsdam.hpi.utils.CollectionUtils;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.IOException;
import java.io.Serializable;
import java.io.Writer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

/**
 * Represents a node in the Functional Dependency (FD) tree.
 */
public class FDTreeElement implements Serializable {

	protected FDTreeElement[] children;
	protected BitSet rhsAttributes;
	protected BitSet rhsFds;
	protected int numAttributes;

	/**
	 * Constructs a new FD tree element with the given number of attributes.
	 *
	 * @param numAttributes the total number of attributes in the dataset
	 */
	public FDTreeElement(int numAttributes) {
		this.rhsAttributes = new BitSet(numAttributes);
		this.rhsFds = new BitSet(numAttributes);
		this.numAttributes = numAttributes;
	}

	/**
	 *
	 * @return number of attributes
	 */
	public int getNumAttributes() {

		return this.numAttributes;
	}
	
	// children

	/**
	 *
	 * @return array of the children nodes
	 */
	public FDTreeElement[] getChildren() {

		return this.children;
	}

	/**
	 *
	 * @param children sets array od {@link FDTreeElement}
	 */
	public void setChildren(FDTreeElement[] children) {

		this.children = children;
	}

	// rhsAttributes

	/**
	 *
	 * @return RHS attributes of this node
	 */
	public BitSet getRhsAttributes() {

		return this.rhsAttributes;
	}

	/**
	 *
	 * @param i adds an attribute to the RHS of this node
	 */
	public void addRhsAttribute(int i) {

		this.rhsAttributes.set(i);
	}

	/**
	 *
	 * @param other adds multiple RHS attributes from {@code other} {@link BitSet}
	 */
	public void addRhsAttributes(BitSet other) {

		this.rhsAttributes.or(other);
	}

	/**
	 *
	 * @param i removes an attribute from the RHS
	 */
	public void removeRhsAttribute(int i) {

		this.rhsAttributes.clear(i);
	}

	/**
	 * Checks if the node contains provided RHS candidate.
	 * @param i chceck attribute in RHS
	 * @return {@code true} if contains; {@code false} otherwise
	 */
	public boolean hasRhsAttribute(int i) {

		return this.rhsAttributes.get(i);
	}
	
	// rhsFds

	/**
	 *
	 * @return {@link BitSet} marking FDs
	 */
	public BitSet getFds() {

		return this.rhsFds;
	}

	/**
	 *
	 * @param i marks specific RHS as a FD
	 */
	public void markFd(int i) {

		this.rhsFds.set(i);
	}

	/**
	 *
	 * @param other marks multiple RHS as FDs
	 */
	public void markFds(BitSet other) {
		this.rhsFds.or(other);
	}

	/**
	 *
	 * @param i removes FD marking
	 */
	public void removeFd(int i) {

		this.rhsFds.clear(i);
	}

	/**
	 *
	 * @param other retains only the FDs present in the provided {@link BitSet}
	 */
	public void retainFds(BitSet other) {

		this.rhsFds.and(other);
	}

	/**
	 *
	 * @param other sets the FDs to exactly the ones in the provided {@link BitSet}
	 */
	public void setFds(BitSet other) {

		this.rhsFds = other;
	}

	/**
	 * Clears all FDs in this node
	 */
	public void removeAllFds() {

		this.rhsFds.clear(0, this.numAttributes);
	}

	/**
	 *
	 * @param i is this RHS attribute a FD?
	 * @return {@code true} if it is FD; {@code false} otherwise
	 */
	public boolean isFd(int i) {

		return this.rhsFds.get(i);
	}

	/**
	 * Recursively trims the tree to the specified depth.
	 * Removes children beyond {@code newDepth} and updates RHS attributes to valid FDs only.
	 */
	protected void trimRecursive(int currentDepth, int newDepth) {
		if (currentDepth == newDepth) {
			this.children = null;
			this.rhsAttributes.and(this.rhsFds);
			return;
		}
		
		if (this.children != null)
			for (FDTreeElement child : this.children)
				if (child != null)
					child.trimRecursive(currentDepth + 1, newDepth);
	}

	/**
	 * Recursively filters generalizations of FDs for this node and updates the FD tree.
	 *
	 * @param currentLhs the current left-hand side attributes
	 * @param tree the FD tree to update
	 */
	protected void filterGeneralizations(BitSet currentLhs, FDTree tree) {
		if (this.children != null) {		
			for (int attr = 0; attr < this.numAttributes; attr++) {
				if (this.children[attr] != null) {
					currentLhs.set(attr);
					this.children[attr].filterGeneralizations(currentLhs, tree);
					currentLhs.clear(attr);
				}
			}
		}
		for (int rhs = this.rhsFds.nextSetBit(0); rhs >= 0; rhs = this.rhsFds.nextSetBit(rhs + 1))
			tree.filterGeneralizations(currentLhs, rhs);
	}

	/**
	 * Filters generalizations of a specific functional dependency lhs -> rhs in the tree.
	 * This removes the rhs from nodes that are generalizations of the given lhs.
	 *
	 * @param lhs the left-hand side attributes of the FD to filter
	 * @param rhs the right-hand side attribute of the FD to filter
	 * @param currentLhsAttr current attribute index in lhs during traversal
	 * @param currentLhs the currently constructed left-hand side during recursion
	 */
	protected void filterGeneralizations(BitSet lhs, int rhs, int currentLhsAttr, BitSet currentLhs) {
		if (currentLhs.equals(lhs))
			return;
		
		this.rhsFds.clear(rhs);
		
		// Is the dependency already read, and we have not yet found a generalization?
		if (currentLhsAttr < 0)
			return;
		
		if (this.children != null) {
			for (int nextLhsAttr = lhs.nextSetBit(currentLhsAttr); nextLhsAttr >= 0; nextLhsAttr = lhs.nextSetBit(nextLhsAttr + 1)) {
				if ((this.children[nextLhsAttr] != null) && (this.children[nextLhsAttr].hasRhsAttribute(rhs))) {
					currentLhs.set(nextLhsAttr);
					this.children[nextLhsAttr].filterGeneralizations(lhs, rhs, lhs.nextSetBit(nextLhsAttr + 1), currentLhs);
					currentLhs.clear(nextLhsAttr);
				}
			}
		}
	}

	/**
	 * Checks if this node or its children contain a FD or generalization of {@code lhs -> rhs}.
	 *
	 * @param lhs the LHS attribute set
	 * @param rhs the RHS attribute
	 * @param currentLhsAttr current attribute index in recursion
	 * @return true if the FD or generalization exists, false otherwise
	 */
	protected boolean containsFdOrGeneralization(BitSet lhs, int rhs, int currentLhsAttr) {
		if (this.isFd(rhs))
			return true;

		// Is the dependency already read and we have not yet found a generalization?
		if (currentLhsAttr < 0)
			return false;
		
		int nextLhsAttr = lhs.nextSetBit(currentLhsAttr + 1);
		
		if ((this.children != null) && (this.children[currentLhsAttr] != null) && (this.children[currentLhsAttr].hasRhsAttribute(rhs)))
			if (this.children[currentLhsAttr].containsFdOrGeneralization(lhs, rhs, nextLhsAttr))
				return true;
		
		return this.containsFdOrGeneralization(lhs, rhs, nextLhsAttr);
	}

	/**
	 * Retrieves a left-hand side that is an FD or generalization of lhs -> rhs.
	 *
	 * @param lhs the left-hand side attributes
	 * @param rhs the right-hand side attribute
	 * @param currentLhsAttr current attribute index during traversal
	 * @param foundLhs BitSet to store the found LHS if exists
	 * @return true if an FD or generalization exists, false otherwise
	 */
	protected boolean getFdOrGeneralization(BitSet lhs, int rhs, int currentLhsAttr, BitSet foundLhs) {
		if (this.isFd(rhs))
			return true;

		// Is the dependency already read and we have not yet found a generalization?
		if (currentLhsAttr < 0)
			return false;
		
		int nextLhsAttr = lhs.nextSetBit(currentLhsAttr + 1);
		
		if ((this.children != null) && (this.children[currentLhsAttr] != null) && (this.children[currentLhsAttr].hasRhsAttribute(rhs))) {
			if (this.children[currentLhsAttr].getFdOrGeneralization(lhs, rhs, nextLhsAttr, foundLhs)) {
				foundLhs.set(currentLhsAttr);
				return true;
			}
		}
		
		return this.getFdOrGeneralization(lhs, rhs, nextLhsAttr, foundLhs);
	}

	/**
	 * Retrieves all left-hand sides that are FDs or generalizations of lhs -> rhs.
	 *
	 * @param lhs the left-hand side attributes to compare
	 * @param rhs the right-hand side attribute
	 * @param currentLhsAttr current attribute index during traversal
	 * @param currentLhs the currently constructed LHS
	 * @param foundLhs list to store found left-hand sides
	 */
	protected void getFdAndGeneralizations(BitSet lhs, int rhs, int currentLhsAttr, BitSet currentLhs, List<BitSet> foundLhs) {
		if (this.isFd(rhs))
			foundLhs.add((BitSet) currentLhs.clone());

		if (this.children == null)
			return;
		
		while (currentLhsAttr >= 0) {
			int nextLhsAttr = lhs.nextSetBit(currentLhsAttr + 1);
			
			if ((this.children[currentLhsAttr] != null) && (this.children[currentLhsAttr].hasRhsAttribute(rhs))) {
				currentLhs.set(currentLhsAttr);
				this.children[currentLhsAttr].getFdAndGeneralizations(lhs, rhs, nextLhsAttr, currentLhs, foundLhs);
				currentLhs.clear(currentLhsAttr);
			}
			
			currentLhsAttr = nextLhsAttr;
		}
	}

	/**
	 * Retrieves all FD tree elements at a specific level.
	 *
	 * @param level the target level in the tree
	 * @param currentLevel current level during traversal
	 * @param currentLhs current left-hand side attributes
	 * @param result list of FDTreeElementLhsPair to collect results
	 */
	public void getLevel(int level, int currentLevel, BitSet currentLhs, List<FDTreeElementLhsPair> result) {
		if (level == currentLevel) {
			result.add(new FDTreeElementLhsPair(this, (BitSet) currentLhs.clone()));
		}
		else {
			currentLevel++;
			if (this.children == null)
				return;
			
			for (int child = 0; child < this.numAttributes; child++) {
				if (this.children[child] == null)
					continue;
				
				currentLhs.set(child);
				this.children[child].getLevel(level, currentLevel, currentLhs, result);
				currentLhs.clear(child);
			}
		}
	}

	/**
	 * Return, whether the tree element contains a specialization of the
	 * functional dependency lhs -> rhs. </br>
	 * 
	 * @param lhs
	 *            The left-hand-side attribute set of the functional dependency.
	 * @param rhs
	 *            The dependent attribute.
	 * @return true, if the element contains a specialization of the functional
	 *         dependency lhs -> a. false otherwise.
	 */
	public boolean containsFdOrSpecialization(BitSet lhs, int rhs) {
		int currentLhsAttr = lhs.nextSetBit(0);
		return this.containsFdOrSpecialization(lhs, rhs, currentLhsAttr);
	}

	/**
	 * Recursively checks for a specialization of {@code lhs -> rhs}.
	 *
	 * @param lhs the LHS attribute set
	 * @param rhs the RHS attribute
	 * @param currentLhsAttr current attribute index
	 * @return true if a specialization exists
	 */
	protected boolean containsFdOrSpecialization(BitSet lhs, int rhs, int currentLhsAttr) {
		if (!this.hasRhsAttribute(rhs))
			return false;
		
		// Is the dependency already covered?
		if (currentLhsAttr < 0)
			return true; // TODO: unsafe if fds can be removed from the tree without adding a specialization of the removed fd. Then, we cannot be sure here: maybe the attributes of the lhs are covered but the current lhs is not part of a valid fd if no isFd() is set for larger lhs in the tree (this might occur if the fd has been removed from the tree)
		
		if (this.children == null)
			return false;
		
		for (int child = 0; child < this.numAttributes; child++) {
			if (this.children[child] == null)
				continue;
			
			if (child == currentLhsAttr) {
				if (this.children[child].containsFdOrSpecialization(lhs, rhs, lhs.nextSetBit(currentLhsAttr + 1)))
					return true;
			}
			else {
				if (this.children[child].containsFdOrSpecialization(lhs, rhs, currentLhsAttr))
					return true;
			}	
		}
		return false;
	}

	/**
	 * Recursively removes the functional dependency lhs -> rhs from the tree.
	 *
	 * @param lhs the left-hand side attributes
	 * @param rhs the right-hand side attribute
	 * @param currentLhsAttr current attribute index in lhs
	 * @return true if removal was successful, false otherwise
	 */
	protected boolean removeRecursive(BitSet lhs, int rhs, int currentLhsAttr) {
		// If this is the last attribute of lhs, remove the fd-mark from the rhs
		if (currentLhsAttr < 0) {
			this.removeFd(rhs);
			this.removeRhsAttribute(rhs);
			return true;
		}
		
		if ((this.children != null) && (this.children[currentLhsAttr] != null)) {
			// Move to the next child with the next lhs attribute
			if (!this.children[currentLhsAttr].removeRecursive(lhs, rhs, lhs.nextSetBit(currentLhsAttr + 1)))
				return false; // This is a shortcut: if the child was unable to remove the rhs, then this node can also not remove it
				
			// Delete the child node if it has no rhs attributes any more
			if (this.children[currentLhsAttr].getRhsAttributes().cardinality() == 0)
				this.children[currentLhsAttr] = null;
		}
		
		// Check if another child requires the rhs and if not, remove it from this node
		if (this.isLastNodeOf(rhs)) {
			this.removeRhsAttribute(rhs);
			return true;
		}
		return false;
	}

	/**
	 * Checks whether this node is the last node containing the given RHS attribute among its children.
	 *
	 * @param rhs the right-hand side attribute
	 * @return true if this node is the last node containing rhs, false otherwise
	 */
	protected boolean isLastNodeOf(int rhs) {
		if (this.children == null)
			return true;
		for (FDTreeElement child : this.children)
			if ((child != null) && child.hasRhsAttribute(rhs))
				return false;
		return true;
	}


	// FUDEBS
	/**
	 * Adds all one-smaller generalizations of the current LHS for a single RHS attribute to the FD tree.
	 *
	 * @param currentLhs the current left-hand side attributes
	 * @param maxCurrentLhsAttribute the maximum attribute index in currentLhs
	 * @param rhs the right-hand side attribute
	 * @param tree the FDTree to which the generalizations are added
	 */
	protected void addOneSmallerGeneralizations(BitSet currentLhs, int maxCurrentLhsAttribute, int rhs, FDTree tree) {
		for (int lhsAttribute = currentLhs.nextSetBit(0); lhsAttribute != maxCurrentLhsAttribute; lhsAttribute = currentLhs.nextSetBit(lhsAttribute + 1)) {
			currentLhs.clear(lhsAttribute);
			tree.addGeneralization(currentLhs, rhs);
			currentLhs.set(lhsAttribute);
		}
	}

	/**
	 * Adds all one-smaller generalizations of the current LHS for multiple RHS attributes to the FD tree.
	 *
	 * @param currentLhs the current left-hand side attributes
	 * @param maxCurrentLhsAttribute the maximum attribute index in currentLhs
	 * @param rhs the right-hand side attributes as BitSet
	 * @param tree the FDTree to which the generalizations are added
	 */
	protected void addOneSmallerGeneralizations(BitSet currentLhs, int maxCurrentLhsAttribute, BitSet rhs, FDTree tree) {
		for (int lhsAttribute = currentLhs.nextSetBit(0); lhsAttribute != maxCurrentLhsAttribute; lhsAttribute = currentLhs.nextSetBit(lhsAttribute + 1)) {
			currentLhs.clear(lhsAttribute);
			tree.addGeneralization(currentLhs, rhs);
			currentLhs.set(lhsAttribute);
		}
	}

	/**
	 * Adds pruned elements (smaller generalizations of current FDs) to the given FDTree.
	 *
	 * @param currentLhs the current left-hand side attributes
	 * @param maxCurrentLhsAttribute the maximum attribute index in currentLhs
	 * @param tree the FDTree where pruned elements are added
	 */
	public void addPrunedElements(BitSet currentLhs, int maxCurrentLhsAttribute, FDTree tree) {
		this.addOneSmallerGeneralizations(currentLhs, maxCurrentLhsAttribute, this.rhsAttributes, tree);
		
		if (this.children == null)
			return;
		
		for (int attr = 0; attr < this.numAttributes; attr++) {
			if (this.children[attr] != null) {
				currentLhs.set(attr);
				this.children[attr].addPrunedElements(currentLhs, attr, tree);
				currentLhs.clear(attr);
			}
		}
	}

	/**
	 * Performs a negative growth of the FD tree.
	 * Generates maximal negative FDs by checking if subsets of the current LHS fail to refine RHS PLIs.
	 *
	 * @param currentPli the current position list index for the LHS
	 * @param currentLhs the current left-hand side attributes
	 * @param maxCurrentLhsAttribute the maximum attribute index in currentLhs
	 * @param plis the list of position list indices for all attributes
	 * @param rhsPlis 2D array of PLIs for RHS attributes
	 * @param invalidFds the FD tree representing invalid FDs
	 */
	public void growNegative(PositionListIndex currentPli, BitSet currentLhs, int maxCurrentLhsAttribute, List<PositionListIndex> plis, int[][] rhsPlis, FDTree invalidFds) {
		int numAttributes = plis.size();

		PositionListIndex[] childPlis = new PositionListIndex[numAttributes];
		
		// I know that I am negative, but I have to check if I am a maximum-negative
		for (int rhs = this.rhsAttributes.nextSetBit(0); rhs >= 0; rhs = this.rhsAttributes.nextSetBit(rhs + 1)) { // For each non-set rhs, we know that a subset of the current lhs must have been a valid fd
			for (int attr = maxCurrentLhsAttribute + 1; attr < numAttributes; attr++) {
				if (attr == rhs)
					continue;
				
				if ((this.children != null) && (this.children[attr] != null) && this.children[attr].hasRhsAttribute(rhs)) // If there is a child with the current rhs, then currentLhs+attr->rhs must already be a known invalid fd
					continue;
				
				if (childPlis[attr] == null)
					childPlis[attr] = currentPli.intersect(rhsPlis[attr]);
				
				if (!childPlis[attr].refines(rhsPlis[rhs])) {
					// Add a new child representing the newly discovered invalid FD
					if (this.children == null)
						this.children = new FDTreeElement[this.numAttributes];
					if (this.children[attr] == null)
						this.children[attr] = new FDTreeElement(this.numAttributes);// Interesting question: Can I generate a new non-FD behind my current position in the tree? Check if attr > allOtherAttr in lhs
					this.children[attr].addRhsAttribute(rhs);
					this.children[attr].markFd(rhs);
					
					// Add all fds of newLhs-size -1 that include the new attribute, because these must also be invalid; use only size -1, because smaller sizes should already exist
					currentLhs.set(attr);
					this.addOneSmallerGeneralizations(currentLhs, attr, rhs, invalidFds);
					currentLhs.clear(attr);
				}
			}
		}
		
		if (this.children != null) {
			// Remove the plis for which no child exists
			for (int i = 0; i < numAttributes; i++)
				if (this.children[i] == null)
					childPlis[i] = null;
		
			// Recursively call children
			for (int attr = 0; attr < numAttributes; attr++) {
				if (this.children[attr] != null) {
					if (childPlis[attr] == null)
						childPlis[attr] = currentPli.intersect(rhsPlis[attr]);
					
					currentLhs.set(attr);
					this.children[attr].growNegative(childPlis[attr], currentLhs, attr, plis, rhsPlis, invalidFds);
					currentLhs.clear(attr);
					
					childPlis[attr] = null;
				}
			}
		}
	}

	/**
	 * Recursively maximizes the negative cover of the FD tree.
	 * Generates maximal negative FDs for all possible extensions of current LHS.
	 *
	 * @param currentPli the current position list index for LHS
	 * @param currentLhs the current left-hand side attributes
	 * @param numAttributes total number of attributes
	 * @param rhsPlis 2D array of PLIs for RHS attributes
	 * @param invalidFds the FD tree representing invalid FDs
	 */
	protected void maximizeNegativeRecursive(PositionListIndex currentPli, BitSet currentLhs, int numAttributes, int[][] rhsPlis, FDTree invalidFds) {
		PositionListIndex[] childPlis = new PositionListIndex[numAttributes];
		
		// Traverse the tree depth-first, left-first; generate plis for children and pass them over; store the child plis locally to reuse them for the checking afterwards
		if (this.children != null) {
			for (int attr = 0; attr < numAttributes; attr++) {
				if (this.children[attr] != null) {
					childPlis[attr] = currentPli.intersect(rhsPlis[attr]);
					
					currentLhs.set(attr);
					this.children[attr].maximizeNegativeRecursive(childPlis[attr], currentLhs, numAttributes, rhsPlis, invalidFds);
					currentLhs.clear(attr);
				}
			}
		}
		
		// On the way back, check all rhs-FDs that all their possible supersets are valid FDs; check with refines or, if available, with previously calculated plis
		//     which supersets to consider: add all attributes A with A notIn lhs and A notequal rhs; 
		//         for newLhs->rhs check that no FdOrSpecialization exists, because it is invalid then; this check might be slower than the FD check on high levels but faster on low levels in particular in the root! this check is faster on the negative cover, because we look for a non-FD
		for (int rhs = this.rhsFds.nextSetBit(0); rhs >= 0; rhs = this.rhsFds.nextSetBit(rhs + 1)) {
			BitSet extensions = (BitSet) currentLhs.clone();
			extensions.flip(0, numAttributes);
			extensions.clear(rhs);
			
			for (int extensionAttr = extensions.nextSetBit(0); extensionAttr >= 0; extensionAttr = extensions.nextSetBit(extensionAttr + 1)) {
				currentLhs.set(extensionAttr);
				if (childPlis[extensionAttr] == null)
					childPlis[extensionAttr] = currentPli.intersect(rhsPlis[extensionAttr]);
				
				// If a superset is a non-FD, mark this as not rhsFD, add the superset as a new node, filterGeneralizations() of the new node, call maximizeNegative() on the new supersets node
				//     if the superset node is in a right node of the tree, it will be checked anyways later; hence, only check supersets that are left or in the same tree path
				if (!childPlis[extensionAttr].refines(rhsPlis[rhs])) {
					this.rhsFds.clear(rhs);

					FDTreeElement newElement = invalidFds.addFunctionalDependency(currentLhs, rhs);
					//invalidFds.filterGeneralizations(currentLhs, rhs); // TODO: test effect
					newElement.maximizeNegativeRecursive(childPlis[extensionAttr], currentLhs, numAttributes, rhsPlis, invalidFds);
				}
				currentLhs.clear(extensionAttr);
			}
		}
	}

	/**
	 * Adds the functional dependencies contained in this tree element into a list of functional dependencies.
	 *
	 * @param functionalDependencies the list to collect functional dependencies
	 * @param lhs the current left-hand side attributes
	 * @param columnIdentifiers the list of column identifiers corresponding to attributes
	 * @param plis the list of position list indices corresponding to attributes
	 */
	public void addFunctionalDependenciesInto(List<_FunctionalDependency> functionalDependencies, BitSet lhs,
											  ObjectArrayList<_ColumnIdentifier> columnIdentifiers, List<PositionListIndex> plis) {
		for (int rhs = this.rhsFds.nextSetBit(0); rhs >= 0; rhs = this.rhsFds.nextSetBit(rhs + 1)) {
			_ColumnIdentifier[] columns = new _ColumnIdentifier[lhs.cardinality()];
			int j = 0;
			for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
				int columnId = plis.get(i).getAttribute(); // Here we translate the column i back to the real column i before the sorting
				columns[j++] = columnIdentifiers.get(columnId); 
			}
			
			_ColumnCombination colCombination = new _ColumnCombination(columns);
			int rhsId = plis.get(rhs).getAttribute(); // Here we translate the column rhs back to the real column rhs before the sorting
			_FunctionalDependency fdResult = new _FunctionalDependency(colCombination, columnIdentifiers.get(rhsId));
			functionalDependencies.add(fdResult);
		}

		if (this.getChildren() == null)
			return;
			
		for (int childAttr = 0; childAttr < this.numAttributes; childAttr++) {
			FDTreeElement element = this.getChildren()[childAttr];
			if (element != null) {
				lhs.set(childAttr);
				element.addFunctionalDependenciesInto(functionalDependencies, lhs, columnIdentifiers, plis);
				lhs.clear(childAttr);
			}
		}
	}

	/**
	 * Recursively adds all valid FDs from this node into a result receiver.
	 *
	 * @param resultReceiver the receiver to collect FDs
	 * @param lhs current LHS attributes
	 * @param columnIdentifiers list of column identifiers
	 * @param plis list of position list indices
	 * @return the number of FDs added
	 */
	public int addFunctionalDependenciesInto(_Input resultReceiver, BitSet lhs, ObjectArrayList<_ColumnIdentifier> columnIdentifiers, List<PositionListIndex> plis) /*throws CouldNotReceiveResultException, ColumnNameMismatchException*/ {
		int numFDs = 0;
		for (int rhs = this.rhsFds.nextSetBit(0); rhs >= 0; rhs = this.rhsFds.nextSetBit(rhs + 1)) {
			_ColumnIdentifier[] columns = new _ColumnIdentifier[lhs.cardinality()];
			int j = 0;
			for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
				int columnId = plis.get(i).getAttribute(); // Here we translate the column i back to the real column i before the sorting
				columns[j++] = columnIdentifiers.get(columnId); 
			}
			
			_ColumnCombination colCombination = new _ColumnCombination(columns);
			int rhsId = plis.get(rhs).getAttribute(); // Here we translate the column rhs back to the real column rhs before the sorting
			_FunctionalDependency fdResult = new _FunctionalDependency(colCombination, columnIdentifiers.get(rhsId));
			resultReceiver.receiveResult(fdResult);
			numFDs++;
		}

		if (this.getChildren() == null)
			return numFDs;
			
		for (int childAttr = 0; childAttr < this.numAttributes; childAttr++) {
			FDTreeElement element = this.getChildren()[childAttr];
			if (element != null) {
				lhs.set(childAttr);
				numFDs += element.addFunctionalDependenciesInto(resultReceiver, lhs, columnIdentifiers, plis);
				lhs.clear(childAttr);
			}
		}
		return numFDs;
	}

	/**
	 * Writes all functional dependencies from this node to a Writer.
	 *
	 * @param writer the Writer to output FDs
	 * @param lhs current left-hand side attributes
	 * @param columnIdentifiers list of column identifiers
	 * @param plis list of position list indices
	 * @param writeTableNamePrefix whether to include table names in output
	 * @return number of FDs written
	 * @throws IOException if writing fails
	 */
	public int writeFunctionalDependencies(Writer writer, BitSet lhs, ObjectArrayList<_ColumnIdentifier> columnIdentifiers, List<PositionListIndex> plis, boolean writeTableNamePrefix) throws IOException {
		int numFDs = this.rhsFds.cardinality();
		
		if (numFDs != 0) {
			List<String> lhsIdentifier = new ArrayList<>();
			for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
				int columnId = plis.get(i).getAttribute(); // Here we translate the column i back to the real column i before the sorting
				if (writeTableNamePrefix)
					lhsIdentifier.add(columnIdentifiers.get(columnId).toString());
				else
					lhsIdentifier.add(columnIdentifiers.get(columnId).getColumnIdentifier());
			}
			Collections.sort(lhsIdentifier);
			String lhsString = "[" + CollectionUtils.concat(lhsIdentifier, ", ") + "]";
			
			List<String> rhsIdentifier = new ArrayList<>();
			for (int i = this.rhsFds.nextSetBit(0); i >= 0; i = this.rhsFds.nextSetBit(i + 1)) {
				int columnId = plis.get(i).getAttribute(); // Here we translate the column i back to the real column i before the sorting
				if (writeTableNamePrefix)
					rhsIdentifier.add(columnIdentifiers.get(columnId).toString());
				else
					rhsIdentifier.add(columnIdentifiers.get(columnId).getColumnIdentifier());
			}
			Collections.sort(rhsIdentifier);
			String rhsString = CollectionUtils.concat(rhsIdentifier, ", ");
			
			writer.write(lhsString + " --> " + rhsString + "\r\n");
		}
			
		if (this.getChildren() == null)
			return numFDs;
			
		for (int childAttr = 0; childAttr < this.numAttributes; childAttr++) {
			FDTreeElement element = this.getChildren()[childAttr];
			if (element != null) {
				lhs.set(childAttr);
				numFDs += element.writeFunctionalDependencies(writer, lhs, columnIdentifiers, plis, writeTableNamePrefix);
				lhs.clear(childAttr);
			}
		}
		return numFDs;
	}

	/**
	 * Recursively filters dead elements (nodes with no FDs) from the tree.
	 *
	 * @return true if this node should be removed, false otherwise
	 */
	public boolean filterDeadElements() {
		boolean allChildrenFiltered = true;
		if (this.children != null) {
			for (int childAttr = 0; childAttr < this.numAttributes; childAttr++) {
				FDTreeElement element = this.children[childAttr];
				if (element != null) {
					if (element.filterDeadElements())
						this.children[childAttr] = null;
					else
						allChildrenFiltered = false;
				}
			}
		}
		return allChildrenFiltered && (this.rhsFds.nextSetBit(0) < 0);
	}

	/**
	 * Helper class representing a pair of a tree element and its associated LHS (left-hand side) BitSet.
	 * Used internally for indexing and traversals.
	 */
	protected class ElementLhsPair {
		public FDTreeElement element = null;
		public BitSet lhs = null;
		public ElementLhsPair(FDTreeElement element, BitSet lhs) {
			this.element = element;
			this.lhs = lhs;
		}
	}


	/**
	 * Adds this node to a level-indexed map for quick retrieval of all nodes at a given depth.
	 *
	 * @param level2elements map from tree level to list of ElementLhsPair objects
	 * @param level current tree level
	 * @param lhs current left-hand side attributes
	 */
	protected void addToIndex(Int2ObjectOpenHashMap<ArrayList<ElementLhsPair>> level2elements, int level, BitSet lhs) {
		level2elements.get(level).add(new ElementLhsPair(this, (BitSet) lhs.clone()));
		if (this.children != null) {
			for (int childAttr = 0; childAttr < this.numAttributes; childAttr++) {
				FDTreeElement element = this.children[childAttr];
				if (element != null) {
					lhs.set(childAttr);
					element.addToIndex(level2elements, level + 1, lhs);
					lhs.clear(childAttr);
				}
			}
		}
	}

	/**
	 * Recursively grows the FD tree by adding all valid specializations.
	 *
	 * @param lhs current left-hand side attributes
	 * @param fdTree the FD tree to update
	 */
	public void grow(BitSet lhs, FDTree fdTree) {
		// Add specializations of all nodes an mark them as isFD, but if specialization exists, then it is invalid and should not be marked; only add specializations of nodes not marked as isFD!
		BitSet rhs = this.rhsAttributes;
		
		BitSet invalidRhs = (BitSet) rhs.clone();
		invalidRhs.andNot(this.rhsFds);
		
		// Add specializations that are not invalid
		if (invalidRhs.cardinality() > 0) {
			for (int extensionAttr = 0; extensionAttr < this.numAttributes; extensionAttr++) {
				if (lhs.get(extensionAttr) || rhs.get(extensionAttr))
					continue;
				
				lhs.set(extensionAttr);
				fdTree.addFunctionalDependencyIfNotInvalid(lhs, invalidRhs);
				lhs.clear(extensionAttr);
			}
		}
		
		// Traverse children and let them add their specializations
		if (this.children != null) {
			for (int childAttr = 0; childAttr < this.numAttributes; childAttr++) {
				FDTreeElement element = this.children[childAttr];
				if (element != null) {
					lhs.set(childAttr);
					element.grow(lhs, fdTree);
					lhs.clear(childAttr);
				}
			}
		}
	}

	/**
	 * Minimizes FDs in this node by removing non-minimal FDs covered by generalizations.
	 *
	 * @param lhs current left-hand side attributes
	 * @param fdTree the FD tree for checking generalizations
	 */
	protected void minimize(BitSet lhs, FDTree fdTree) {
		// Traverse children and minimize their FDs
		if (this.children != null) {
			for (int childAttr = 0; childAttr < this.numAttributes; childAttr++) {
				FDTreeElement element = this.children[childAttr];
				if (element != null) {
					lhs.set(childAttr);
					element.minimize(lhs, fdTree);
					lhs.clear(childAttr);
				}
			}
		}
		
		// Minimize Fds by checking for generalizations
		for (int rhs = this.rhsFds.nextSetBit(0); rhs >= 0; rhs = this.rhsFds.nextSetBit(rhs + 1)) {
			this.rhsFds.clear(rhs);
			
			// If the FD was minimal, i.e. no generalization exists, set it again
			if (!fdTree.containsFdOrGeneralization(lhs, rhs))
				this.rhsFds.set(rhs);
		}
	}
	

}
