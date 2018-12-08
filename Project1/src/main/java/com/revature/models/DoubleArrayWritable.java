package com.revature.models;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

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
