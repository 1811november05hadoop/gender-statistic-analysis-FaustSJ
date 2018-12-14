package com.revature.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.FemaleGraduatesMapper;
import com.revature.reduce.LessThan30Reducer;

public class FemaleGradLessThan30Test {
	//with mapper generics
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	//output of mapper and output of mapper
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	//mrunit mapreduce
	//input of mapper, output of mapper which happens to be input of reducer
	//		and then out put of the reducer
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	
	@Before
	public void setUp() {
		FemaleGraduatesMapper mapper = new FemaleGraduatesMapper();
		mapDriver = new MapDriver<>(); //types are inferred
		mapDriver.setMapper(mapper);
		
		LessThan30Reducer reducer = new LessThan30Reducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	//with input, with output
	//insertions are built behind the scenes
	@Test
	public void testMapper() {
		//giving it a psudo-file line
		mapDriver.withInput(new LongWritable(1), new Text("\"Bhutan\",\"BTN\",\"School enrollment, tertiary, female (% gross)\",\"SE.TER.ENRR.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.19124\",\"0.42282\",\"0.39417\",\"\",\"\",\"0.25595\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"2.01428\",\"\",\"\",\"\",\"\",\"\",\"3.72243\",\"3.70851\",\"3.82923\",\"4.70295\",\"4.73191\",\"5.28042\",\"7.08169\",\"7.67794\",\"9.23651\",\"\",\"\",\"\","));
		//expected output
		mapDriver.withOutput(new Text("Bhutan\n\t female tertiary school enrollment for 2013 (% gross): "), new DoubleWritable(9.23651));
		//run the test
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() {
		List<DoubleWritable> values = new ArrayList<>();
		values.add(new DoubleWritable(9.23651));
		
		//mock input (in place of Context context
		reduceDriver.withInput(new Text("Bhutan\n\t female tertiary school enrollment for 2013 (% gross): "), values);
		//expected output
		reduceDriver.withOutput(new Text("Bhutan\n\t female tertiary school enrollment for 2013 (% gross): "), new DoubleWritable(9.23651));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		//mapDriver's input
		mapReduceDriver.withInput(new LongWritable(1), new Text("\"Bhutan\",\"BTN\",\"School enrollment, tertiary, female (% gross)\",\"SE.TER.ENRR.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.19124\",\"0.42282\",\"0.39417\",\"\",\"\",\"0.25595\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"2.01428\",\"\",\"\",\"\",\"\",\"\",\"3.72243\",\"3.70851\",\"3.82923\",\"4.70295\",\"4.73191\",\"5.28042\",\"7.08169\",\"7.67794\",\"9.23651\",\"\",\"\",\"\","));
		
		//reduceDriver's output
		mapReduceDriver.withOutput(new Text("Bhutan\n\t female tertiary school enrollment for 2013 (% gross): "), new DoubleWritable(9.23651));
		
		//run() can return a list of key,value pairs for manual testing
		mapReduceDriver.runTest();
	}
}

