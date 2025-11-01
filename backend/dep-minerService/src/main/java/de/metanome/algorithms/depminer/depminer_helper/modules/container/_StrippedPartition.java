package de.metanome.algorithms.depminer.depminer_helper.modules.container;

import cz.cuni.mff.fdfinder.depminerservice.algorithm.util._LongBitSet;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongList;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Represents a stripped partition for a specific attribute in a relational dataset.
 */
public class _StrippedPartition implements Serializable{

	private final int attribute;
	private final List<LongList> value = new LinkedList<>();
	private boolean finalized = false;
	private int indexOfPartition = 0;
	private Long2IntOpenHashMap partitionOfID = new Long2IntOpenHashMap();

	/**
	 * Constructs a stripped partition for the given attribute.
	 */
	public _StrippedPartition(int attribute) {

		this.attribute = attribute;
                
                partitionOfID.defaultReturnValue(-1);
	}

	/**
	 * Adds a single equivalence class (a {@link LongList} of tuple IDs) to this stripped partition.
	 * Also updates the tuple-to-partition lookup map.
	 */
	public void addElement(LongList element) {

		if (finalized) {
			return;
		}
		this.value.add(element);
                
		for (long elem : element){

			this.partitionOfID.put(elem, this.indexOfPartition);
		}
		this.indexOfPartition++;
	}

	/**
	 * Adds multiple equivalence classes at once to this stripped partition.
	 */
	public void addElements(List<LongList> elements){

		if (finalized) {
			return;
		}
		this.value.addAll(elements);

		Long s = 0L;
		for (LongList el : elements) {

			for (long elem : el){

				this.partitionOfID.put(elem, this.indexOfPartition);
			}
			s+=el.size();
			this.indexOfPartition++;

		}
		//System.out.println("ATT: "+this.attribute+" NUM ID: "+s+" NUM Keys: "+this.partitionOfID.keySet().size());
	}

	/**
	 * Checks whether two tuple IDs belong to the same equivalence class for this attribute.
	 *
	 * @param firstID {@link Long} ID of the first tuple
	 * @param secondID {@link Long} ID of the second tuple
	 * @return {@code true} if both tuples belong to the same equivalence class; {@code false} otherwise
	 */
	public boolean isFirstInSamePartitionAsSecond(long firstID, long secondID){

		int firstPartitionIndex = this.getIndexOfPartition(firstID);
		int secondPartitionIndex = this.getIndexOfPartition(secondID);
		//System.out.println("INDICES: "+firstPartitionIndex + " "+secondPartitionIndex);

		return firstID != secondID
				&& firstPartitionIndex > -1
				&& secondPartitionIndex > -1
				//&& Objects.equals(firstPartitionIndex, secondPartitionIndex);
				&& firstPartitionIndex == secondPartitionIndex;
	}

	/**
	 * Prints all (tupleID → partitionIndex) mappings — useful for debugging.
	 */
	private void printHash(){
		System.out.println("Printing HashSet for ATT: "+this.attribute+"...");
		for(long l :partitionOfID.keySet()){
				System.out.println("HashSet: K "+l+" -> V "+partitionOfID.get(l));
			}
		System.out.println("DONE");
	}

	/**
	 * Marks this partition as finalized, disallowing further modification.
	 */
	public void markFinalized() {

		this.finalized = true;
	}

	/**
	 * Returns the attribute ID represented by this stripped partition.
	 */
	public int getAttributeID() {

		return this.attribute;
	}

	/**
	 * Returns the list of equivalence classes (as LongLists).
	 */
	public List<LongList> getValues() {

		return this.value;

	}

	/**
	 * @returns {@link Integer} partition index (equivalence class index) for a given {@link Long} tuple ID.
	 * If the tuple ID is not found, returns -1.
	 */
	private Integer getIndexOfPartition(long id){

		return this.partitionOfID.get(id);
	}

	/**
	 * Converts the internal representation of equivalence classes into a List of {@link _LongBitSet}.
	 * Each BitSet represents one equivalence class with bits set at positions corresponding to tuple IDs.
	 * @return {@link List} of equivalence classes
	 */
	public List<_LongBitSet> getValuesAsBitSet() {

		List<_LongBitSet> result = new LinkedList<>();
		for (LongList list : this.value) {
			_LongBitSet set = new _LongBitSet();
			for (long i : list) {
				set.set(i);
			}
			result.add(set);
		}
		return result;
	}

	@Override
	public String toString() {

		String s = "sp(";
		for (LongList il : this.value) {
			s += il.toString() + "-";
		}
		return s + ")";
	}

	/**
	* Creates a shallow copy of this stripped partition.
	*/
	public _StrippedPartition copy() {
		System.out.println("PROBIHA COPY V STRIPPED PARTITION!");
		_StrippedPartition copy = new _StrippedPartition(this.attribute);
		for (LongList l : this.value) {
			copy.value.add(l);
		}
		copy.finalized = this.finalized;
		return copy;
	}

}
