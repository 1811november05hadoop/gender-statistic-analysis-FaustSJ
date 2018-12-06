package com.revature.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.WordMapper;
import com.revature.reduce.SumReducer;

public class WordCountTest {
/*
 * you need 3 classes, 
 * 3 drivers, mock classes to test mapreduce
 * 
 * map driver, reduce driver, and mapreduce driver
 */
	
	//with mapper generics
	private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	//output of mapper and output of mapper
	private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	//mrunit mapreduce
	//input of mapper, output of mapper which happens to be input of reducer
	//		and then out put of the reducer
	private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	
	@Before
	public void setUp() {
		WordMapper mapper = new WordMapper();
		mapDriver = new MapDriver<>(); //types are inferred
		mapDriver.setMapper(mapper);
		
		SumReducer reducer = new SumReducer();
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
		mapDriver.withInput(new LongWritable(1), new Text("cat cat dog"));
			
		//expected output (a word and a 1)
		mapDriver.withOutput(new Text("cat"), new IntWritable(1));
		mapDriver.withOutput(new Text("cat"), new IntWritable(1));
		mapDriver.withOutput(new Text("dog"), new IntWritable(1));
		//withOutput is like addOutput, but returns itself for chaining
		/*
		 * mapDriver.withOutput(...).withOutput(...).withOutput(...)
		 * is the same thing as above
		 */
		
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() {
		List<IntWritable> values = new ArrayList<>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		
		//mock input (in place of Context context
		reduceDriver.withInput(new Text("cat"), values);
		//expected output
		reduceDriver.withOutput(new Text("cat"), new IntWritable(2));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		//mapDriver's input
		mapReduceDriver.withInput(new LongWritable(1), new Text("cat cat dog"));
		
		//reduceDriver's output
		mapReduceDriver.withOutput(new Text("cat"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("dog"), new IntWritable(1));
		
		//run() can return a list of key,value pairs for manual testing
		mapReduceDriver.runTest();
	}
}









