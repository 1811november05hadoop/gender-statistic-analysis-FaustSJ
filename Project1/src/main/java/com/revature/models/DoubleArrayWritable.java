package com.revature.models;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

/*
 * https://hadoop.apache.org/docs/r2.7.1/api/org/apache/hadoop/io/ArrayWritable.html
 * https://stackoverflow.com/questions/28914596/mapreduce-output-arraywritable
 */

public class DoubleArrayWritable extends ArrayWritable{
	
	//for some reason a no-args constructor is needed...?
	//error in YearlyDifferenceReducer, line 20
	public DoubleArrayWritable(){
		super(DoubleWritable.class);
	}
	
	public DoubleArrayWritable(DoubleWritable[] values) {
		super(DoubleWritable.class, values);
	}
	
	@Override
	public DoubleWritable[] get() {
		//returns an array of type DoubleWritable
		DoubleWritable[] dw = new DoubleWritable[super.get().length];
		int index = 0;
		for(Writable writable : super.get()){
			dw[index] = (DoubleWritable) writable;
			index++;
		}
		//return (DoubleWritable[]) super.get();  ERROR
		return dw;
	}
	
	//Properly prints the array
	@Override
	public String toString(){
		DoubleWritable[] elems = this.get();
		StringBuilder toReturn = new StringBuilder("");
		for(DoubleWritable dw : elems){
			toReturn.append(" "+dw.toString());
		}
		return toReturn.toString();
	}

	//hashCode() and equals() MUST be overridden in order for the MRUnit tests 
	//		to properly compare results with objects of this class as output.
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(this.get());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DoubleArrayWritable other = (DoubleArrayWritable) obj;
		DoubleWritable[] da = other.get();
		DoubleWritable[] thisda = this.get();
		//if the arrays don't match in length, return false
		if(da.length==thisda.length){
			int index = 0;
			//If the values don't match, return false
			for(DoubleWritable dw : thisda){
				if(dw.get()!=da[index].get()){				
					return false;
				}
				++index;
			}
		}
		return true;
	}	
}
