package de.metanome.algorithms.hyfd;

import de.metanome.algorithms.hyfd.structures.FDList;
import de.metanome.algorithms.hyfd.structures.FDSet;
import de.metanome.algorithms.hyfd.structures.FDTree;

import java.io.Serializable;
import java.lang.management.ManagementFactory;

/**
 * The {@link MemoryGuardian} class monitors JVM memory usage and ensures
 * that memory-intensive data structures such as {@link FDSet}, {@link FDTree},
 * and {@link FDList} do not exceed predefined memory thresholds.
 */
public class MemoryGuardian implements Serializable {
	
	private boolean active;
	private final float maxMemoryUsagePercentage = 0.8f;	// Memory usage in percent from which a lattice level should be dropped
	private final float trimMemoryUsagePercentage = 0.7f;	// If data structures must be trimmed, this is the memory percentage that they are trimmed to (trim to less than max memory usage to avoid oscillating trimming)
	private long memoryCheckFrequency;						// Number of allocation events that cause a memory check
	private long maxMemoryUsage;
	private long trimMemoryUsage;
	private long availableMemory;
	private int allocationEventsSinceLastCheck = 0;

	/**
	 * Sets whether the memory guardian is active.
	 *
	 * @param active {@code true} to activate memory monitoring, {@code false} to deactivate.
	 */
	public void setActive(boolean active) {

		this.active = active;
	}

	/**
	 * Checks whether memory guarding is active.
	 *
	 * @return {@code true} if active, {@code false} otherwise.
	 */
	public boolean isActive() {

		return this.active;
	}

	/**
	 * Constructs a {@code MemoryGuardian} instance with the specified activation status.
	 * Initializes memory thresholds based on the JVM's maximum heap memory.
	 *
	 * @param active Whether the memory guardian is active.
	 */
	public MemoryGuardian(boolean active) {
		this.active = active;
		this.availableMemory = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
		this.maxMemoryUsage = (long)(this.availableMemory * this.maxMemoryUsagePercentage);
		this.trimMemoryUsage = (long)(this.availableMemory * this.trimMemoryUsagePercentage);
		this.memoryCheckFrequency = (long)Math.max(Math.ceil((float)this.availableMemory / 10000000), 10);
	}

	/**
	 * Updates the number of allocation events since the last memory check.
	 * Should be called whenever data structures are modified or new objects are allocated.
	 *
	 * @param allocationEvents Number of allocation events to add.
	 */
	public void memoryChanged(int allocationEvents) {

		this.allocationEventsSinceLastCheck += allocationEvents;
	}

	/**
	 * Determines whether the current heap memory usage exceeds the specified threshold.
	 *
	 * @param memory Memory threshold in bytes.
	 * @return {@code true} if the current heap memory usage exceeds {@code memory}, {@code false} otherwise.
	 */
	public boolean memoryExhausted(long memory) {

		long memoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
		return memoryUsage > memory;
	}

	/**
	 * Checks memory usage and trims or garbage collects data structures if necessary.
	 * Only runs if memory guarding is active and the allocation event threshold has been reached.
	 *
	 * @param negCover {@link FDSet} representing the negative cover.
	 * @param posCover {@link FDTree} representing the positive cover.
	 * @param newNonFDs {@link FDList} containing newly discovered non-FDs (can be {@code null}).
	 * @throws RuntimeException if there is not enough memory to retain any lattice levels.
	 */
	public void match(FDSet negCover, FDTree posCover, FDList newNonFDs) {
		if ((!this.active) || (this.allocationEventsSinceLastCheck < this.memoryCheckFrequency))
			return;
		
		if (this.memoryExhausted(this.maxMemoryUsage)) {
//			Logger.getInstance().writeln("Memory exhausted (" + ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() + "/" + this.maxMemoryUsage + ") ");
			Runtime.getRuntime().gc();
//			Logger.getInstance().writeln("GC reduced to " + ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
			
			while (this.memoryExhausted(this.trimMemoryUsage)) {
				int depth = Math.max(posCover.getDepth(), negCover.getDepth()) - 1;
				if (depth < 1)
					throw new RuntimeException("Insufficient memory to calculate any result!");
				
				System.out.print(" (trim to " + depth + ")");
				posCover.trim(depth);
				negCover.trim(depth);
				if (newNonFDs != null)
					newNonFDs.trim(depth);
				Runtime.getRuntime().gc();
			}
		}
		
		this.allocationEventsSinceLastCheck = 0;
	}
}
