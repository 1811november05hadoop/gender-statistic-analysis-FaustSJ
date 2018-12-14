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

import com.revature.map.GlobalMaleEmploymentSince2000Mapper;
import com.revature.models.DoubleArrayWritable;
import com.revature.reduce.DifferenceReducer;

public class PercentChangeInMaleEmpFrom2000Test {
	//with mapper generics
	private MapDriver<LongWritable, Text, Text, DoubleArrayWritable> mapDriver;
	//output of mapper and output of mapper
	private ReduceDriver<Text, DoubleArrayWritable, Text, Text> reduceDriver;
	//mrunit mapreduce
	//input of mapper, output of mapper which happens to be input of reducer
	//		and then out put of the reducer
	private MapReduceDriver<LongWritable, Text, Text, DoubleArrayWritable, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {
		GlobalMaleEmploymentSince2000Mapper mapper = new GlobalMaleEmploymentSince2000Mapper();
		mapDriver = new MapDriver<>(); //types are inferred
		mapDriver.setMapper(mapper);

		DifferenceReducer reducer = new DifferenceReducer();
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
		mapDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Employment to population ratio, 15+, male (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"69.302001953125\",\"68.802001953125\",\"69.0240020751953\",\"69.5419998168945\",\"69.9820022583008\",\"70.1650009155273\",\"70.6579971313477\",\"70.9810028076172\",\"71.1549987792969\",\"71.2910003662109\",\"70.1220016479492\",\"68.7910003662109\",\"67.9130020141602\",\"68.1800003051758\",\"68.5139999389648\",\"69.0100021362305\",\"68.6940002441406\",\"67.463996887207\",\"63.5250015258789\",\"62.6809997558594\",\"62.9599990844727\",\"63.6580009460449\",\"63.6969985961914\",\"64.2409973144531\",\"64.7129974365234\",\"64.8519973754883\",\r\n"));
		//expected output
		DoubleWritable y1 = new DoubleWritable(71.2910003662109);
		DoubleWritable y2 = new DoubleWritable(64.8519973754883);
		DoubleWritable[] dw = {y1, y2};
		mapDriver.withOutput(new Text("United States, change in the % of males employed,\n\t2000 compared to 2016: "), new DoubleArrayWritable(dw));
		//run the test
		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		//making the mock input
		DoubleWritable y1 = new DoubleWritable(71.2910003662109);
		DoubleWritable y2 = new DoubleWritable(64.8519973754883);
		DoubleWritable[] dw = {y1, y2};
		List<DoubleArrayWritable> values = new ArrayList<>();
		values.add(new DoubleArrayWritable(dw));

		//mock input (in place of Context context
		reduceDriver.withInput(new Text("United States, change in the % of males employed,\n\t2000 compared to 2016: "), values);
		//expected output
		double dif = (double)Math.round(((y2.get()-y1.get())*-1) * 100000d) / 100000d;
		reduceDriver.withOutput(new Text("United States, change in the % of males employed,\n\t2000 compared to 2016: "), new Text("employment percentage decreased by "+dif));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		//mapDriver's input
		mapReduceDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Employment to population ratio, 15+, male (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"69.302001953125\",\"68.802001953125\",\"69.0240020751953\",\"69.5419998168945\",\"69.9820022583008\",\"70.1650009155273\",\"70.6579971313477\",\"70.9810028076172\",\"71.1549987792969\",\"71.2910003662109\",\"70.1220016479492\",\"68.7910003662109\",\"67.9130020141602\",\"68.1800003051758\",\"68.5139999389648\",\"69.0100021362305\",\"68.6940002441406\",\"67.463996887207\",\"63.5250015258789\",\"62.6809997558594\",\"62.9599990844727\",\"63.6580009460449\",\"63.6969985961914\",\"64.2409973144531\",\"64.7129974365234\",\"64.8519973754883\",\r\n"));
		
		//reduceDriver's output
		DoubleWritable y1 = new DoubleWritable(71.2910003662109);
		DoubleWritable y2 = new DoubleWritable(64.8519973754883);
		double dif = (double)Math.round(((y2.get()-y1.get())*-1) * 100000d) / 100000d;
		mapReduceDriver.withOutput(new Text("United States, change in the % of males employed,\n\t2000 compared to 2016: "), new Text("employment percentage decreased by "+dif));

		//run() can return a list of key,value pairs for manual testing
		mapReduceDriver.runTest();
	}
}

