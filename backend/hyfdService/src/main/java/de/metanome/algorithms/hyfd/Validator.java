package de.metanome.algorithms.hyfd;

import de.metanome.algorithms.hyfd.structures.*;
import de.metanome.algorithms.hyfd.utils.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * The {@code Validator} class is responsible for validating functional dependencies (FDs)
 * against a dataset using {@link PositionListIndex} (PLIs) and a combination of positive and
 * negative covers ({@link FDTree} and {@link FDSet}).
 */
public class Validator implements Serializable {

	private FDSet negCover;
	private FDTree posCover;
	private int numRecords;
	private List<PositionListIndex> plis;
	private int[][] compressedRecords;
	private float efficiencyThreshold;
	private MemoryGuardian memoryGuardian;
	//private ExecutorService executor;
	private static JavaSparkContext context;
	
	private int level = 0;

	/**
	 * Constructs a new {@code Validator}.
	 *
	 * @param negCover The {@link FDSet} representing the negative cover.
	 * @param posCover The {@link FDTree} representing the positive cover.
	 * @param numRecords Number of records in the dataset.
	 * @param compressedRecords Dataset in compressed representation.
	 * @param plis List of {@link PositionListIndex} for each attribute.
	 * @param efficiencyThreshold Threshold for early termination during validation.
	 * @param parallel Whether to execute validation in parallel (Spark).
	 * @param memoryGuardian {@link MemoryGuardian} instance for memory management.
	 * @param context {@link JavaSparkContext} for Spark execution.
	 */
	public Validator(FDSet negCover, FDTree posCover, int numRecords, int[][] compressedRecords, List<PositionListIndex> plis, float efficiencyThreshold, boolean parallel, MemoryGuardian memoryGuardian, JavaSparkContext context) {
		this.negCover = negCover;
		this.posCover = posCover;
		this.numRecords = numRecords;
		this.plis = plis;
		this.compressedRecords = compressedRecords;
		this.efficiencyThreshold = efficiencyThreshold;
		this.memoryGuardian = memoryGuardian;
		this.context = context;

	}

	/**
	 * Represents a functional dependency (FD) with a left-hand side (LHS) and right-hand side (RHS).
	 */
	private class FD implements Serializable{
		public BitSet lhs;
		public int rhs;

		/**
		 * Constructs a new {@code FD}.
		 *
		 * @param lhs Left-hand side as a {@link BitSet}.
		 * @param rhs Right-hand side attribute.
		 */
		public FD(BitSet lhs, int rhs) {
			this.lhs = lhs;
			this.rhs = rhs;
		}
	}

	/**
	 * Represents the result of a validation task, storing the number of validations,
	 * intersections, invalid FDs, and comparison suggestions.
	 */
	private class ValidationResult implements Serializable{
		public int validations = 0;
		public int intersections = 0;
		public List<FD> invalidFDs = new ArrayList<>();
		public List<IntegerPair> comparisonSuggestions = new ArrayList<>();

		/**
		 * Adds another {@link ValidationResult} to this one.
		 *
		 * @param other Another {@link ValidationResult} instance.
		 */
		public void add(ValidationResult other) {
			this.validations += other.validations;
			this.intersections += other.intersections;
			this.invalidFDs.addAll(other.invalidFDs);
			this.comparisonSuggestions.addAll(other.comparisonSuggestions);
		}
	}

	/**
	 * Task for validating a single FDTree element with a specific LHS.
	 * Supports sequential execution and Spark parallel execution.
	 */
	private class ValidationTask implements Callable<ValidationResult> {
		private FDTreeElementLhsPair elementLhsPair;

		/**
		 * Sets the element-LHS pair for validation.
		 *
		 * @param elementLhsPair {@link FDTreeElementLhsPair} to validate.
		 */
		public void setElementLhsPair(FDTreeElementLhsPair elementLhsPair) {

			this.elementLhsPair = elementLhsPair;
		}

		/**
		 * Constructs a {@link ValidationTask} for a given FDTree element and LHS pair.
		 *
		 * @param elementLhsPair {@link FDTreeElementLhsPair} to validate.
		 */
		public ValidationTask(FDTreeElementLhsPair elementLhsPair) {

			this.elementLhsPair = elementLhsPair;
		}

		/**
		 * Executes the validation for this task.
		 *
		 * @return {@link ValidationResult} containing invalid FDs, intersections, and validations count.
		 * @throws Exception if validation fails.
		 */
		public ValidationResult call() throws Exception {
			ValidationResult result = new ValidationResult();
			
			FDTreeElement element = this.elementLhsPair.getElement();
			BitSet lhs = this.elementLhsPair.getLhs();
			BitSet rhs = element.getFds();
			
			int rhsSize = rhs.cardinality();
			if (rhsSize == 0)
				return result;
			result.validations = result.validations + rhsSize;
			
			if (Validator.this.level == 0) {
				// Check if rhs is unique
				for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1)) {
					if (!Validator.this.plis.get(rhsAttr).isConstant(Validator.this.numRecords)) {
						element.removeFd(rhsAttr);
						result.invalidFDs.add(new FD(lhs, rhsAttr));
					}
					result.intersections++;
				}
			}
			else if (Validator.this.level == 1) {
				// Check if lhs from plis refines rhs
				int lhsAttribute = lhs.nextSetBit(0);
				for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1)) {
					if (!Validator.this.plis.get(lhsAttribute).refines(Validator.this.compressedRecords, rhsAttr)) {
						element.removeFd(rhsAttr);
						result.invalidFDs.add(new FD(lhs, rhsAttr));
					}
					result.intersections++;
				}
			}
			else {
				// Check if lhs from plis plus remaining inverted plis refines rhs
				int firstLhsAttr = lhs.nextSetBit(0);
				
				lhs.clear(firstLhsAttr);
				BitSet validRhs = Validator.this.plis.get(firstLhsAttr).refines(Validator.this.compressedRecords, lhs, rhs, result.comparisonSuggestions);
				lhs.set(firstLhsAttr);
				
				result.intersections++;
				
				rhs.andNot(validRhs); // Now contains all invalid FDs
				element.setFds(validRhs); // Sets the valid FDs in the FD tree
				
				for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1))
					result.invalidFDs.add(new FD(lhs, rhsAttr));
			}
			return result;
		}
	}

	/**
	 * Validates a list of FDTree elements sequentially.
	 *
	 * @param currentLevel List of {@link FDTreeElementLhsPair} representing the current level.
	 * @return {@link ValidationResult} with all validations and invalid FDs.
	 */
	private ValidationResult validateSequential(List<FDTreeElementLhsPair> currentLevel) /*throws AlgorithmExecutionException */{
		ValidationResult validationResult = new ValidationResult();
		
		ValidationTask task = new ValidationTask(null);
		for (FDTreeElementLhsPair elementLhsPair : currentLevel) {
			task.setElementLhsPair(elementLhsPair);
			try {
				validationResult.add(task.call());
			}
			catch (Exception e) {
				e.printStackTrace();
				//throw new AlgorithmExecutionException(e.getMessage());
			}
		}
		
		return validationResult;
	}

	/**
	 * Validates a list of FDTree elements using Spark parallelization.
	 *
	 * @param currentLevel List of {@link FDTreeElementLhsPair} representing the current level.
	 * @return {@link ValidationResult} with all validations and invalid FDs.
	 */
	private ValidationResult validateSpark (List<FDTreeElementLhsPair> currentLevel) {
		JavaRDD<FDTreeElementLhsPair> currentLevelRDD = context.parallelize(currentLevel);
		return currentLevelRDD
			.map(elementLhsPair -> {
				ValidationResult result = new ValidationResult();

				FDTreeElement element = elementLhsPair.getElement();
				BitSet lhs = elementLhsPair.getLhs();
				BitSet rhs = element.getFds();

				int rhsSize = rhs.cardinality();
				if (rhsSize == 0)
					return result;
				result.validations = result.validations + rhsSize;

				if (Validator.this.level == 0) {
					// Check if rhs is unique
					for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1)) {
						if (!Validator.this.plis.get(rhsAttr).isConstant(Validator.this.numRecords)) {
							element.removeFd(rhsAttr);
							result.invalidFDs.add(new FD(lhs, rhsAttr));
						}
						result.intersections++;
					}
				}
				else if (Validator.this.level == 1) {
					// Check if lhs from plis refines rhs
					int lhsAttribute = lhs.nextSetBit(0);
					for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1)) {
						if (!Validator.this.plis.get(lhsAttribute).refines(Validator.this.compressedRecords, rhsAttr)) {
							element.removeFd(rhsAttr);
							result.invalidFDs.add(new FD(lhs, rhsAttr));
						}
						result.intersections++;
					}
				}
				else {
					// Check if lhs from plis plus remaining inverted plis refines rhs
					int firstLhsAttr = lhs.nextSetBit(0);

					lhs.clear(firstLhsAttr);
					BitSet validRhs = Validator.this.plis.get(firstLhsAttr).refines(Validator.this.compressedRecords, lhs, rhs, result.comparisonSuggestions);
					lhs.set(firstLhsAttr);

					result.intersections++;

					rhs.andNot(validRhs); // Now contains all invalid FDs
					element.setFds(validRhs); // Sets the valid FDs in the FD tree

					for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1))
						result.invalidFDs.add(new FD(lhs, rhsAttr));
				}
				return result;
			})
			.reduce((valResult1, valResult2) -> {
				valResult1.add(valResult2);
				return valResult1;
			})
			;
	}

	/**
	 * Performs a level-wise validation of the positive cover using PLIs.
	 * It may generate additional FD candidates based on invalid FDs found.
	 *
	 * @return List of {@link IntegerPair} representing suggested comparisons for future validation.
	 */
	public List<IntegerPair> validatePositiveCover() /*throws AlgorithmExecutionException */{
		int numAttributes = this.plis.size();
		
		Logger.getInstance().writeln("Validating FDs using plis ...");
		
		List<FDTreeElementLhsPair> currentLevel = null;
		if (this.level == 0) {
			currentLevel = new ArrayList<>();
			currentLevel.add(new FDTreeElementLhsPair(this.posCover, new BitSet(numAttributes)));
		}
		else {
			currentLevel = this.posCover.getLevel(this.level);
		}
		
		// Start the level-wise validation/discovery
		int previousNumInvalidFds = 0;
		List<IntegerPair> comparisonSuggestions = new ArrayList<>();
		while (!currentLevel.isEmpty()) {
			Logger.getInstance().write("\tLevel " + this.level + ": " + currentLevel.size() + " elements; ");
			
			// Validate current level
			Logger.getInstance().write("(V)");
			
			//ValidationResult validationResult = (this.executor == null) ? this.validateSequential(currentLevel) : this.validateSpark(currentLevel);
			ValidationResult validationResult = this.validateSpark(currentLevel);
			comparisonSuggestions.addAll(validationResult.comparisonSuggestions);
			
			// If the next level exceeds the predefined maximum lhs size, then we can stop here
			if ((this.posCover.getMaxDepth() > -1) && (this.level >= this.posCover.getMaxDepth())) {
				int numInvalidFds = validationResult.invalidFDs.size();
				int numValidFds = validationResult.validations - numInvalidFds;
				Logger.getInstance().writeln("(-)(-); " + validationResult.intersections + " intersections; " + validationResult.validations + " validations; " + numInvalidFds + " invalid; " + "-" + " new candidates; --> " + numValidFds + " FDs");
				break;
			}
			
			// Add all children to the next level
			Logger.getInstance().write("(C)");
			
			List<FDTreeElementLhsPair> nextLevel = new ArrayList<>();
			for (FDTreeElementLhsPair elementLhsPair : currentLevel) {
				FDTreeElement element = elementLhsPair.getElement();
				BitSet lhs = elementLhsPair.getLhs();

				if (element.getChildren() == null)
					continue;
				
				for (int childAttr = 0; childAttr < numAttributes; childAttr++) {
					FDTreeElement child = element.getChildren()[childAttr];
					
					if (child != null) {
						BitSet childLhs = (BitSet) lhs.clone();
						childLhs.set(childAttr);
						nextLevel.add(new FDTreeElementLhsPair(child, childLhs));
					}
				}
			}
						
			// Generate new FDs from the invalid FDs and add them to the next level as well
			Logger.getInstance().write("(G); ");
			
			int candidates = 0;
			for (FD invalidFD : validationResult.invalidFDs) {
				for (int extensionAttr = 0; extensionAttr < numAttributes; extensionAttr++) {
					BitSet childLhs = this.extendWith(invalidFD.lhs, invalidFD.rhs, extensionAttr);
					if (childLhs != null) {
						FDTreeElement child = this.posCover.addFunctionalDependencyGetIfNew(childLhs, invalidFD.rhs);
						if (child != null) {
							nextLevel.add(new FDTreeElementLhsPair(child, childLhs));
							candidates++;
							
							this.memoryGuardian.memoryChanged(1);
							this.memoryGuardian.match(this.negCover, this.posCover, null);
						}
					}
				}
				
				if ((this.posCover.getMaxDepth() > -1) && (this.level >= this.posCover.getMaxDepth()))
					break;
			}
			
			currentLevel = nextLevel;
			this.level++;
			int numInvalidFds = validationResult.invalidFDs.size();
			int numValidFds = validationResult.validations - numInvalidFds;
			Logger.getInstance().writeln(validationResult.intersections + " intersections; " + validationResult.validations + " validations; " + numInvalidFds + " invalid; " + candidates + " new candidates; --> " + numValidFds + " FDs");
		
			// Decide if we continue validating the next level or if we go back into the sampling phase
			if ((numInvalidFds > numValidFds * this.efficiencyThreshold) && (previousNumInvalidFds < numInvalidFds))
				return comparisonSuggestions;
			//	return new ArrayList<>();
			previousNumInvalidFds = numInvalidFds;
		}

		return null;
	}

	/**
	 * Extends a given LHS with an additional attribute for candidate generation.
	 * Performs pruning to avoid non-minimal FDs.
	 *
	 * @param lhs Original LHS as {@link BitSet}.
	 * @param rhs RHS attribute.
	 * @param extensionAttr Attribute to extend LHS with.
	 * @return New {@link BitSet} representing extended LHS, or {@code null} if pruning rejects it.
	 */
	private BitSet extendWith(BitSet lhs, int rhs, int extensionAttr) {
		if (lhs.get(extensionAttr) || 											// Triviality: AA->C cannot be valid, because A->C is invalid
			(rhs == extensionAttr) || 											// Triviality: AC->C cannot be valid, because A->C is invalid
			this.posCover.containsFdOrGeneralization(lhs, extensionAttr) ||		// Pruning: If A->B, then AB->C cannot be minimal // TODO: this pruning is not used in the Inductor when inverting the negCover; so either it is useless here or it is useful in the Inductor?
			((this.posCover.getChildren() != null) && (this.posCover.getChildren()[extensionAttr] != null) && this.posCover.getChildren()[extensionAttr].isFd(rhs)))	
																				// Pruning: If B->C, then AB->C cannot be minimal
			return null;
		
		BitSet childLhs = (BitSet) lhs.clone(); // TODO: This clone() could be avoided when done externally
		childLhs.set(extensionAttr);
		
		// TODO: Add more pruning here
		
		// if contains FD: element was a child before and has already been added to the next level
		// if contains Generalization: element cannot be minimal, because generalizations have already been validated
		if (this.posCover.containsFdOrGeneralization(childLhs, rhs))										// Pruning: If A->C, then AB->C cannot be minimal
			return null;
		
		return childLhs;
	}

}
