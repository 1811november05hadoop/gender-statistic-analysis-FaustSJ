package com.revature.models;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

/*
 * https://hadoop.apache.org/docs/r2.7.1/api/org/apache/hadoop/io/ArrayWritable.html
 * https://stackoverflow.com/questions/28914596/mapreduce-output-arraywritable
 */

public class DoubleArrayWritable extends ArrayWritable{

	public DoubleArrayWritable(DoubleWritable[] values) {
		super(DoubleWritable.class, values);
	}
	
	@Override
	public DoubleWritable[] get() {
		//returns an array of type DoubleWritable
		return (DoubleWritable[]) super.get();
	}
}
