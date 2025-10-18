/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package cz.cuni.mff.fdfinder.depminerservice.algorithm.model;

import cz.cuni.mff.fdfinder.depminerservice.algorithm.util._LongBitSet;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongList;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author pavel.koupil
 */
public class _StrippedPartition implements Serializable{

	private final int attribute;
	private final List<LongList> value = new LinkedList<>();
	private boolean finalized = false;
        private int indexOfPartition = 0;
        private Long2IntOpenHashMap partitionOfID = new Long2IntOpenHashMap();
               

	public _StrippedPartition(int attribute) {

		this.attribute = attribute;
                
                partitionOfID.defaultReturnValue(-1);
	}

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
        
        private void printHash(){
            System.out.println("Printing HashSet for ATT: "+this.attribute+"...");
            for(long l :partitionOfID.keySet()){
                    System.out.println("HashSet: K "+l+" -> V "+partitionOfID.get(l));
                }
            System.out.println("DONE");
        }

	public void markFinalized() {

		this.finalized = true;
	}

	public int getAttributeID() {

		return this.attribute;
	}

	public List<LongList> getValues() {

		return this.value;

	}
        
        private Integer getIndexOfPartition(long id){
            
            return this.partitionOfID.get(id);
        }

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
