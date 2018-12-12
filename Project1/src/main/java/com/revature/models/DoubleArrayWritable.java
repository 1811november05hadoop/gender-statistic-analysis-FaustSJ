package com.revature.models;

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
}
