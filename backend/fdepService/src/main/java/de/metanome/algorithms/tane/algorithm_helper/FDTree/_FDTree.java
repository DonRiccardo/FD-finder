package de.metanome.algorithms.tane.algorithm_helper.FDTree;

import java.io.Serializable;
import java.util.BitSet;

/**
 * Represents the root of a Functional Dependency (FD) tree.
 */
public class _FDTree extends _FDTreeElement implements Serializable{

    /**
     * Constructs a new Functional Dependency Tree with a specified maximum number of attributes.
     *
     * @param maxAttributeNumber the maximum number of attributes in the dataset.
     */
    public _FDTree(int maxAttributeNumber) {

        super(maxAttributeNumber);
    }

    /**
     * Adds the most general dependencies to the FD tree.
     */
    public void addMostGeneralDependencies() {
        this.rhsAttributes.set(1, maxAttributeNumber + 1);
        for (int i = 0; i < maxAttributeNumber; i++) {
            isfd[i] = true;
        }
    }

    /**
     * Adds a new functional dependency (FD) of the form {@code lhs → a} into the FD tree.
     *
     * @param lhs the left-hand-side (LHS) attribute set of the dependency, represented as a {@link BitSet}.
     *            Each bit position corresponds to an attribute index.
     * @param a   the right-hand-side (RHS) attribute that is dependent on {@code lhs}.
     */
    public void addFunctionalDependency(BitSet lhs, int a) {
        _FDTreeElement fdTreeEl;
        //update root vertex
        _FDTreeElement currentNode = this;
        currentNode.addRhsAttribute(a);

        for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {

            if (currentNode.children[i - 1] == null) {
                fdTreeEl = new _FDTreeElement(maxAttributeNumber);
                currentNode.children[i - 1] = fdTreeEl;
            }
            // update vertex to add attribute
            currentNode = currentNode.getChild(i - 1);
            currentNode.addRhsAttribute(a);
        }
        // mark the last element
        currentNode.markAsLastVertex(a - 1);
    }

    /**
     * Checks whether the FD tree is empty.
     * <p>
     * A tree is considered empty if it does not contain any right-hand-side (RHS) attributes.
     * </p>
     *
     * @return {@code true} if the tree contains no functional dependencies; {@code false} otherwise.
     */
    public boolean isEmpty() {

        return (rhsAttributes.cardinality() == 0);
    }

    /**
     * Filters the FD tree by removing all functional dependencies that are specializations
     * of other dependencies already present in the tree.
     * <p>
     * The resulting tree will contain only the most general dependencies.
     * </p>
     */
    public void filterSpecializations() {
        BitSet activePath = new BitSet();
        _FDTree filteredTree = new _FDTree(maxAttributeNumber);
        this.filterSpecializations(filteredTree, activePath);

        this.children = filteredTree.children;
        this.isfd = filteredTree.isfd;

    }

    /**
     * Filters the FD tree by removing all generalizations of existing dependencies.
     * <p>
     * This ensures that only the most specific dependencies remain in the resulting FD tree.
     * </p>
     */
    public void filterGeneralizations() {
        BitSet activePath = new BitSet();
        _FDTree filteredTree = new _FDTree(maxAttributeNumber);
        this.filterGeneralizations(filteredTree, activePath);

        this.children = filteredTree.children;
    }

    /**
     * Prints all functional dependencies stored in the FD tree to the standard output.
     * <p>
     * Each dependency is printed in the format {@code {lhs} -> rhs}, where {@code lhs}
     * is a comma-separated list of attribute indices.
     * </p>
     */
    public void printDependencies() {
        BitSet activePath = new BitSet();
        this.printDependencies(activePath);

    }

}
