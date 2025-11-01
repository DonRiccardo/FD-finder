package de.metanome.algorithms.hyfd.structures;

import cz.cuni.mff.fdfinder.hyfdservice.algorithm.model._Input;
import de.metanome.algorithms.depminer.depminer_helper.modules.container._FunctionalDependency;
import de.metanome.algorithms.depminer.depminer_helper.modules.container._FunctionalDependency._ColumnIdentifier;
import de.uni_potsdam.hpi.utils.FileUtils;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Represents a Functional Dependency (FD) tree structure that stores, queries, and manages
 * discovered functional dependencies and their generalizations.
 */
public class FDTree extends FDTreeElement {

	private int depth = 0;
	private int maxDepth;

	/**
	 * Constructs a new {@code FDTree} with a specified number of attributes and maximum search depth.
	 *
	 * @param numAttributes total number of attributes in the dataset
	 * @param maxDepth maximum allowed depth of this tree
	 */
	public FDTree(int numAttributes, int maxDepth) {
		super(numAttributes);
		this.maxDepth = maxDepth;
		this.children = new FDTreeElement[numAttributes];
	}

	/**
	 * Returns the current depth of the tree.
	 *
	 * @return the maximum depth reached in the tree so far
	 */
	public int getDepth() {

		return this.depth;
	}

	/**
	 * Returns the maximum allowed depth of this FD tree.
	 *
	 * @return the maximum depth limit
	 */
	public int getMaxDepth() {

		return this.maxDepth;
	}

	@Override
	public String toString() {
		return "[" + this.depth + " depth, " + this.maxDepth + " maxDepth]";
	}

	/**
	 * Trims the FD tree to a specified depth, removing all deeper nodes.
	 *
	 * @param newDepth the target depth to which the tree will be trimmed
	 */
	public void trim(int newDepth) {
		this.trimRecursive(0, newDepth);
		this.depth = newDepth;
		this.maxDepth = newDepth;
	}

	/**
	 * Marks all possible dependencies (LHS → RHS) as valid by setting all RHS bits.
	 */
	public void addMostGeneralDependencies() {
		this.rhsAttributes.set(0, this.numAttributes);
		this.rhsFds.set(0, this.numAttributes);
	}

	/**
	 * Adds a single functional dependency (LHS → RHS) to the tree.
	 *
	 * @param lhs the left-hand side attributes
	 * @param rhs the right-hand side attribute
	 * @return the {@link FDTreeElement} corresponding to the newly added or updated dependency
	 */
	public FDTreeElement addFunctionalDependency(BitSet lhs, int rhs) {
		FDTreeElement currentNode = this;
		currentNode.addRhsAttribute(rhs);

		int lhsLength = 0;
		for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
			lhsLength++;
			
			if (currentNode.getChildren() == null) {
				currentNode.setChildren(new FDTreeElement[this.numAttributes]);
				currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
			}
			else if (currentNode.getChildren()[i] == null) {
				currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
			}
				
			currentNode = currentNode.getChildren()[i];
			currentNode.addRhsAttribute(rhs);
		}
		currentNode.markFd(rhs);
		
		this.depth = Math.max(this.depth, lhsLength);
		return currentNode;
	}

	/**
	 * Adds a multi-valued functional dependency (LHS → multiple RHS) to the tree.
	 *
	 * @param lhs the left-hand side attributes
	 * @param rhs the right-hand side attributes
	 * @return the {@link FDTreeElement} representing the inserted dependencies
	 */
	public FDTreeElement addFunctionalDependency(BitSet lhs, BitSet rhs) {
		FDTreeElement currentNode = this;
		currentNode.addRhsAttributes(rhs);

		int lhsLength = 0;
		for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
			lhsLength++;
			
			if (currentNode.getChildren() == null) {
				currentNode.setChildren(new FDTreeElement[this.numAttributes]);
				currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
			}
			else if (currentNode.getChildren()[i] == null) {
				currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
			}
				
			currentNode = currentNode.getChildren()[i];
			currentNode.addRhsAttributes(rhs);
		}
		currentNode.markFds(rhs);
		
		this.depth = Math.max(this.depth, lhsLength);
		return currentNode;
	}

	/**
	 * Adds a functional dependency only if it introduces a new element in the tree.
	 *
	 * @param lhs the left-hand side attributes
	 * @param rhs the right-hand side attribute
	 * @return the new {@link FDTreeElement} if the dependency is new, otherwise {@code null}
	 */
	public FDTreeElement addFunctionalDependencyGetIfNew(BitSet lhs, int rhs) {
		FDTreeElement currentNode = this;
		currentNode.addRhsAttribute(rhs);

		boolean isNew = false;
		int lhsLength = 0;
		for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
			lhsLength++;
			
			if (currentNode.getChildren() == null) {
				currentNode.setChildren(new FDTreeElement[this.numAttributes]);
				currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
				isNew = true;
			}
			else if (currentNode.getChildren()[i] == null) {
				currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
				isNew = true;
			}
				
			currentNode = currentNode.getChildren()[i];
			currentNode.addRhsAttribute(rhs);
		}
		currentNode.markFd(rhs);

		this.depth = Math.max(this.depth, lhsLength);
		if (isNew)
			return currentNode;
		return null;
	}

	/**
	 * Adds functional dependencies that are not marked as invalid by existing entries.
	 *
	 * @param lhs the left-hand side attributes
	 * @param rhs the right-hand side attributes
	 * @return the {@link FDTreeElement} where the FD was added
	 */
	public FDTreeElement addFunctionalDependencyIfNotInvalid(BitSet lhs, BitSet rhs) {
		FDTreeElement currentNode = this;
		currentNode.addRhsAttributes(rhs);

		BitSet invalidFds = (BitSet) currentNode.rhsAttributes.clone();
		int lhsLength = 0;
		for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
			lhsLength++;
			
			if (currentNode.getChildren() == null) {
				currentNode.setChildren(new FDTreeElement[this.numAttributes]);
				currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
			}
			else if (currentNode.getChildren()[i] == null) {
				currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
			}
				
			currentNode = currentNode.getChildren()[i];
			invalidFds.and(currentNode.rhsFds);
			currentNode.addRhsAttributes(rhs);
		}
		
		rhs.andNot(invalidFds);
		currentNode.markFds(rhs);
		rhs.or(invalidFds);

		this.depth = Math.max(this.depth, lhsLength);
		return currentNode;
	}

	/**
	 * Checks whether the tree already contains the given functional dependency (LHS → RHS).
	 *
	 * @param lhs the left-hand side attributes
	 * @param rhs the right-hand side attribute
	 * @return {@code true} if the FD is contained, {@code false} otherwise
	 */
	public boolean containsFd(BitSet lhs, int rhs) {
		FDTreeElement element = this;
		for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
			if ((element.getChildren() == null) || (element.getChildren()[i] == null))
				return false;
			element = element.getChildren()[i];
		}
		return element.isFd(rhs);
	}

	/**
	 * Adds a generalization of a functional dependency (e.g., removes one or more LHS attributes).
	 *
	 * @param lhs the generalized left-hand side
	 * @param rhs the right-hand side attribute
	 * @return the new {@link FDTreeElement} if added, otherwise {@code null}
	 */
	public FDTreeElement addGeneralization(BitSet lhs, int rhs) {
		FDTreeElement currentNode = this;
		currentNode.addRhsAttribute(rhs);

		boolean newElement = false;
		for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
			if (currentNode.getChildren() == null) {
				currentNode.setChildren(new FDTreeElement[this.numAttributes]);
				currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
				newElement = true;
			}
			else if (currentNode.getChildren()[i] == null) {
				currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
				newElement = true;
			}
			
			currentNode = currentNode.getChildren()[i];
			currentNode.addRhsAttribute(rhs);
		}
		
		if (newElement)
			return currentNode;
		return null;
	}

	/**
	 * Adds generalizations for multiple right-hand side attributes.
	 *
	 * @param lhs the generalized left-hand side
	 * @param rhs the right-hand side attributes
	 * @return the new {@link FDTreeElement} if any generalization was added, otherwise {@code null}
	 */
	public FDTreeElement addGeneralization(BitSet lhs, BitSet rhs) {
		FDTreeElement currentNode = this;
		currentNode.addRhsAttributes(rhs);

		boolean newElement = false;
		for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
			if (currentNode.getChildren() == null) {
				currentNode.setChildren(new FDTreeElement[this.numAttributes]);
				currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
				newElement = true;
			}
			else if (currentNode.getChildren()[i] == null) {
				currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
				newElement = true;
			}
				
			currentNode = currentNode.getChildren()[i];
			currentNode.addRhsAttributes(rhs);
		}
		
		if (newElement)
			return currentNode;
		return null;
	}

	/**
	 * Checks whether the tree contains a given FD or one of its generalizations.
	 *
	 * @param lhs the left-hand side attributes
	 * @param rhs the right-hand side attribute
	 * @return {@code true} if the FD or a generalization exists, otherwise {@code false}
	 */
	public boolean containsFdOrGeneralization(BitSet lhs, int rhs) {
		int nextLhsAttr = lhs.nextSetBit(0);
		return this.containsFdOrGeneralization(lhs, rhs, nextLhsAttr);
	}

	/**
	 * Retrieves the most specific generalization of a given functional dependency.
	 *
	 * @param lhs the left-hand side attributes
	 * @param rhs the right-hand side attribute
	 * @return the generalized {@link BitSet} if found, otherwise {@code null}
	 */
	public BitSet getFdOrGeneralization(BitSet lhs, int rhs) {
		BitSet foundLhs = new BitSet();
		int nextLhsAttr = lhs.nextSetBit(0);
		if (this.getFdOrGeneralization(lhs, rhs, nextLhsAttr, foundLhs))
			return foundLhs;
		return null;
	}

	/**
	 * Returns all generalizations (and the exact FD if present) for a given functional dependency.
	 *
	 * @param lhs the left-hand side attributes
	 * @param rhs the right-hand side attribute
	 * @return list of {@link BitSet}s representing all generalizations of the FD
	 */
	public List<BitSet> getFdAndGeneralizations(BitSet lhs, int rhs) {
		List<BitSet> foundLhs = new ArrayList<>();
		BitSet currentLhs = new BitSet();
		int nextLhsAttr = lhs.nextSetBit(0);
		this.getFdAndGeneralizations(lhs, rhs, nextLhsAttr, currentLhs, foundLhs);
		return foundLhs;
	}

	/**
	 * Retrieves all elements at a given tree level.
	 *
	 * @param level the desired depth level
	 * @return list of {@link FDTreeElementLhsPair} objects representing the level
	 */
	public List<FDTreeElementLhsPair> getLevel(int level) {
		List<FDTreeElementLhsPair> result = new ArrayList<>();
		BitSet currentLhs = new BitSet();
		int currentLevel = 0;
		this.getLevel(level, currentLevel, currentLhs, result);
		return result;
	}

	/**
	 * Removes redundant generalizations in the tree to retain only minimal FDs.
	 */
	public void filterGeneralizations() {
		// Traverse the tree depth-first
		// For each node, iterate all FDs
		// For each FD, store the FD, then remove all this.getFdOrGeneralization(lhs, rhs) and add the FD again
		BitSet currentLhs = new BitSet(this.numAttributes);
		this.filterGeneralizations(currentLhs, this);
	}

	/**
	 * Filters generalizations of a specific functional dependency.
	 *
	 * @param lhs the left-hand side attributes
	 * @param rhs the right-hand side attribute
	 */
	public void filterGeneralizations(BitSet lhs, int rhs) {
		BitSet currentLhs = new BitSet(this.numAttributes);
		int nextLhsAttr = lhs.nextSetBit(0);
		this.filterGeneralizations(lhs, rhs, nextLhsAttr, currentLhs);
	}

	/**
	 * Removes a specific functional dependency from the tree.
	 *
	 * @param lhs the left-hand side attributes
	 * @param rhs the right-hand side attribute
	 */
	public void removeFunctionalDependency(BitSet lhs, int rhs) {
		int currentLhsAttr = lhs.nextSetBit(0);
		this.removeRecursive(lhs, rhs, currentLhsAttr);
	}

	/**
	 * Checks whether a specific functional dependency is contained in the tree.
	 *
	 * @param lhs the left-hand side attributes
	 * @param rhs the right-hand side attribute
	 * @return {@code true} if contained, {@code false} otherwise
	 */
	public boolean containsFunctionalDependency(BitSet lhs, int rhs) {
		FDTreeElement currentNode = this;

		for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
			if ((currentNode.getChildren() == null) || (currentNode.getChildren()[i] == null))
				return false;
			
			currentNode = currentNode.getChildren()[i];
		}
		
		return currentNode.isFd(rhs);
	}

	/**
	 * Returns whether the FD tree contains no dependencies.
	 *
	 * @return {@code true} if empty, {@code false} otherwise
	 */
	public boolean isEmpty() {

		return (this.rhsAttributes.cardinality() == 0);
	}

	// FUDEBS
	/**
	 * Represents a simple FD entry consisting of LHS, RHS, and the corresponding PLI.
	 */
	public class FD {
		public BitSet lhs;
		public int rhs;
		public PositionListIndex pli;
		public FD(BitSet lhs, int rhs, PositionListIndex pli) {
			this.lhs = lhs;
			this.rhs = rhs;
			this.pli = pli;
		}
	}

	/**
	 * Adds pruned (filtered) elements back into the tree.
	 */
	public void addPrunedElements() {
		int numAttributes = this.numAttributes;
		
		BitSet currentLhs = new BitSet(numAttributes);
		
		if (this.getChildren() == null)
			return;
		
		for (int attr = 0; attr < this.numAttributes; attr++) {
			if (this.getChildren()[attr] != null) {
				currentLhs.set(attr);
				this.getChildren()[attr].addPrunedElements(currentLhs, attr, this);
				currentLhs.clear(attr);
			}
		}
	}

	/**
	 * Expands the negative cover of the tree by identifying invalid FDs.
	 *
	 * @param plis list of {@link PositionListIndex} objects for all attributes
	 * @param invertedPlis precomputed inverted PLIs
	 * @param numRecords total number of records in the dataset
	 */
	public void growNegative(List<PositionListIndex> plis, int[][] invertedPlis, int numRecords) {
		int numAttributes = plis.size();
		
		BitSet currentLhs = new BitSet(numAttributes);
		
		// As root node, we need to check each rhs
		for (int rhs = 0; rhs < numAttributes; rhs++) {
			if (this.isFd(rhs)) // Is already invalid?
				continue;
			
			if (plis.get(rhs).isConstant(numRecords)) // Is {} -> rhs a valid FD
				continue;
			this.markFd(rhs);
			
			for (int attr = 0; attr < numAttributes; attr++) {
				if (attr == rhs)
					continue;
				
				if ((this.getChildren() != null) && (this.getChildren()[attr] != null) && this.getChildren()[attr].hasRhsAttribute(rhs)) // If there is a child with the current rhs, then currentLhs+attr->rhs must already be a known invalid fd
					continue;
					
				if (!plis.get(attr).refines(invertedPlis[rhs])) {
					// Add a new child representing the newly discovered invalid FD
					if (this.getChildren() == null)
						this.setChildren(new FDTreeElement[this.numAttributes]);
					if (this.getChildren()[attr] == null)
						this.getChildren()[attr] = new FDTreeElement(this.numAttributes);
					this.getChildren()[attr].addRhsAttribute(rhs);
					this.getChildren()[attr].markFd(rhs);
				}
			}
		}
		
		// Recursively call children
		if (this.getChildren() == null)
			return;
		for (int attr = 0; attr < numAttributes; attr++) {
			if (this.getChildren()[attr] != null) {
				currentLhs.set(attr);
				this.getChildren()[attr].growNegative(plis.get(attr), currentLhs, attr, plis, invertedPlis, this);
				currentLhs.clear(attr);
			}
		}
	}

	/**
	 * Maximizes the negative cover by adding all non-FDs that are generalizations of existing ones.
	 *
	 * @param plis list of {@link PositionListIndex} objects
	 * @param invertedPlis precomputed inverted PLIs
	 * @param numRecords number of records in the dataset
	 */
	public void maximizeNegative(List<PositionListIndex> plis, int[][] invertedPlis, int numRecords) {
		// Maximizing negative cover is better than maximizing positive cover, because we do not need to check minimality; inversion does this automatically, i.e., generating a non-FD that creates a valid, non-minimal FD is not possible
		int numAttributes = plis.size();
		BitSet currentLhs = new BitSet(numAttributes);
		
		// Traverse the tree depth-first, left-first
		if (this.getChildren() != null) {
			for (int attr = 0; attr < numAttributes; attr++) {
				if (this.getChildren()[attr] != null) {
					currentLhs.set(attr);
					this.getChildren()[attr].maximizeNegativeRecursive(plis.get(attr), currentLhs, numAttributes, invertedPlis, this);
					currentLhs.clear(attr);
				}
			}
		}

		// Add invalid root FDs {} -/-> rhs to negative cover, because these are seeds for not yet discovered non-FDs
		this.addInvalidRootFDs(plis, numRecords); // TODO: These FDs make the search complex again :-(

		// On the way back, check all rhs-FDs that all their possible supersets are valid FDs; check with refines or, if available, with previously calculated plis
		//     which supersets to consider: add all attributes A with A notIn lhs and A notequal rhs; 
		//         for newLhs->rhs check that no FdOrSpecialization exists, because it is invalid then; this check might be slower than the FD check on high levels but faster on low levels in particular in the root! this check is faster on the negative cover, because we look for a non-FD
		for (int rhs = this.rhsFds.nextSetBit(0); rhs >= 0; rhs = this.rhsFds.nextSetBit(rhs + 1)) {
			BitSet extensions = (BitSet) currentLhs.clone();
			extensions.flip(0, numAttributes);
			extensions.clear(rhs);
			
			// If a superset is a non-FD, mark this as not rhsFD, add the superset as a new node, filterGeneralizations() of the new node, call maximizeNegative() on the new supersets node
			//     if the superset node is in a right node of the tree, it will be checked anyways later; hence, only check supersets that are left or in the same tree path
			for (int extensionAttr = extensions.nextSetBit(0); extensionAttr >= 0; extensionAttr = extensions.nextSetBit(extensionAttr + 1)) {
				currentLhs.set(extensionAttr);
				
				if (this.containsFdOrSpecialization(currentLhs, rhs) || !plis.get(extensionAttr).refines(invertedPlis[rhs])) { // Otherwise, it must be false and a specialization is already contained; Only needed in root node, because we already filtered generalizations of other nodes and use a depth-first search that always removes generalizations when a new node comes in!
					this.rhsFds.clear(rhs);
					
					FDTreeElement newElement = this.addFunctionalDependency(currentLhs, rhs);
					//this.filterGeneralizations(currentLhs, rhs); // TODO: test effect
					newElement.maximizeNegativeRecursive(plis.get(extensionAttr), currentLhs, numAttributes, invertedPlis, this);
				}
				currentLhs.clear(extensionAttr);
			}
		}
	}

	/**
	 * Adds invalid root-level FDs (those with empty LHS) to the tree.
	 *
	 * @param plis list of {@link PositionListIndex} objects
	 * @param numRecords total number of records in the dataset
	 */
	protected void addInvalidRootFDs(List<PositionListIndex> plis, int numRecords) {
		// Root node: Check all attributes if they are unique; if A is not unique, mark this.isFD(A) as a non-FD; we need these seeds for a complete maximization
		for (int rhs = 0; rhs < this.numAttributes; rhs++)
			if (!plis.get(rhs).isConstant(numRecords)) // Is {} -> rhs an invalid FD
				this.markFd(rhs);
	}

	/**
	 * Collects all valid functional dependencies from this FD tree.
	 *
	 * @param columnIdentifiers identifiers for all columns
	 * @param plis list of {@link PositionListIndex} objects
	 * @return list of {@link _FunctionalDependency} objects representing valid FDs
	 */
	public List<_FunctionalDependency> getFunctionalDependencies(ObjectArrayList<_ColumnIdentifier> columnIdentifiers, List<PositionListIndex> plis) {
		List<_FunctionalDependency> functionalDependencies = new ArrayList<>();
		this.addFunctionalDependenciesInto(functionalDependencies, new BitSet(), columnIdentifiers, plis);
		return functionalDependencies;
	}

	/**
	 * Adds all functional dependencies into a provided result receiver.
	 *
	 * @param resultReceiver receiver for collecting functional dependencies
	 * @param columnIdentifiers column identifiers
	 * @param plis position list indices
	 * @return the number of added functional dependencies
	 */
	public int addFunctionalDependenciesInto(_Input resultReceiver, ObjectArrayList<_ColumnIdentifier> columnIdentifiers, List<PositionListIndex> plis) /*throws CouldNotReceiveResultException, ColumnNameMismatchException*/ {
		return this.addFunctionalDependenciesInto(resultReceiver, new BitSet(), columnIdentifiers, plis);
	}

	/**
	 * Writes all discovered functional dependencies into an output file.
	 *
	 * @param outputFilePath path to the file to write
	 * @param columnIdentifiers list of column identifiers
	 * @param plis list of position list indices
	 * @param writeTableNamePrefix whether to prefix column names with table names
	 * @return the number of FDs written
	 */
	public int writeFunctionalDependencies(String outputFilePath, ObjectArrayList<_ColumnIdentifier> columnIdentifiers, List<PositionListIndex> plis, boolean writeTableNamePrefix) {
		Writer writer = null;
		int numFDs = 0;
		try {
			writer = FileUtils.buildFileWriter(outputFilePath, false);
			numFDs = this.writeFunctionalDependencies(writer, new BitSet(), columnIdentifiers, plis, writeTableNamePrefix);
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			if (writer != null) {
				try {
					writer.close();
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return numFDs;
	}

	/**
	 * Expands the FD tree by adding generalizations for all existing nodes.
	 */
	public void generalize() {
		int maxLevel = this.numAttributes;
		
		// Build an index level->nodes for the top-down, level-wise traversal
		Int2ObjectOpenHashMap<ArrayList<ElementLhsPair>> level2elements = new Int2ObjectOpenHashMap<>(maxLevel);
		for (int level = 0; level < maxLevel; level++)
			level2elements.put(level, new ArrayList<>());
		this.addToIndex(level2elements, 0, new BitSet(this.numAttributes));
		
		// Traverse the levels top-down and add all direct generalizations
		for (int level = maxLevel - 1; level >= 0; level--) {
			for (ElementLhsPair pair : level2elements.get(level)) {
				// Remove isFDs, because we will mark valid FDs later on
				pair.element.removeAllFds();
				
				// Generate and add generalizations
				for (int lhsAttr = pair.lhs.nextSetBit(0); lhsAttr >= 0; lhsAttr = pair.lhs.nextSetBit(lhsAttr + 1)) {
					pair.lhs.clear(lhsAttr);
					FDTreeElement generalization = this.addGeneralization(pair.lhs, pair.element.getRhsAttributes());
					if (generalization != null)
						level2elements.get(level - 1).add(new ElementLhsPair(generalization, (BitSet) pair.lhs.clone()));
					pair.lhs.set(lhsAttr);
				}
			}
		}
	}

	/**
	 * Grows the FD tree recursively.
	 */
	public void grow() {

		this.grow(new BitSet(this.numAttributes), this);
	}

	/**
	 * Minimizes the FD tree by removing redundant dependencies.
	 */
	public void minimize() {

		this.minimize(new BitSet(this.numAttributes), this);
	}

}
