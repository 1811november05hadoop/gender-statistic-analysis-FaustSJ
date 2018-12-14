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

import com.revature.map.TertiaryEducationGPIMapper;
import com.revature.models.DoubleArrayWritable;
import com.revature.reduce.GPIDifferenceReducer;

public class GPIIncreaseInFiveYearsTest {

	//with mapper generics
	private MapDriver<LongWritable, Text, Text, DoubleArrayWritable> mapDriver;
	//output of mapper and output of mapper
	private ReduceDriver<Text, DoubleArrayWritable, Text, DoubleWritable> reduceDriver;
	//mrunit mapreduce
	//input of mapper, output of mapper which happens to be input of reducer
	//		and then out put of the reducer
	private MapReduceDriver<LongWritable, Text, Text, DoubleArrayWritable, Text, DoubleWritable> mapReduceDriver;

	@Before
	public void setUp() {
		TertiaryEducationGPIMapper mapper = new TertiaryEducationGPIMapper();
		mapDriver = new MapDriver<>(); //types are inferred
		mapDriver.setMapper(mapper);

		GPIDifferenceReducer reducer = new GPIDifferenceReducer();
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
		mapDriver.withInput(new LongWritable(1), new Text("\"Guinea\",\"GIN\",\"School enrollment, tertiary (gross), gender parity index (GPI)\",\"SE.ENR.TERT.FM.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.08769\",\"\",\"\",\"\",\"0.14314\",\"0.21649\",\"0.23748\",\"0.23668\",\"\",\"\",\"0.23402\",\"0.27195\",\"0.27685\",\"\",\"0.34187\",\"0.16668\",\"0.15703\",\"0.13152\",\"0.11281\",\"0.08964\",\"0.06933\",\"0.068\",\"0.0743\",\"0.06707\",\"\",\"0.09591\",\"0.12041\",\"0.13302\",\"\",\"\",\"\",\"\",\"\",\"0.18688\",\"0.23138\",\"0.27449\",\"0.29156\",\"0.3272\",\"0.33645\",\"0.33047\",\"0.35398\",\"0.36977\",\"0.43901\",\"0.44725\",\"\",\"\",\r\n"));
		//expected output
		DoubleWritable y1 = new DoubleWritable(0.33047);
		DoubleWritable y2 = new DoubleWritable(0.44725);
		DoubleWritable[] dw = {y1, y2};
		//stats[0] + ", " + stats[2] + ", from "+furthestYear+" to "+latestYear+": "
		mapDriver.withOutput(new Text("Guinea, tertiary school enrollment\nGPI increase from 2010 to 2014: "), new DoubleArrayWritable(dw));
		//run the test
		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		//making the mock input
		DoubleWritable y1 = new DoubleWritable(0.33047);
		DoubleWritable y2 = new DoubleWritable(0.44725);
		DoubleWritable[] dw = {y1, y2};
		List<DoubleArrayWritable> values = new ArrayList<>();
		values.add(new DoubleArrayWritable(dw));

		//mock input (in place of Context context
		reduceDriver.withInput(new Text("Guinea, tertiary school enrollment\nGPI increase from 2010 to 2014: "), values);
		//expected output
		double dif = y2.get()-y1.get();
		reduceDriver.withOutput(new Text("Guinea, tertiary school enrollment\nGPI increase from 2010 to 2014: "), new DoubleWritable((double)Math.round((dif) * 100000d) / 100000d));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		//mapDriver's input
		mapReduceDriver.withInput(new LongWritable(1), new Text("\"Guinea\",\"GIN\",\"School enrollment, tertiary (gross), gender parity index (GPI)\",\"SE.ENR.TERT.FM.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.08769\",\"\",\"\",\"\",\"0.14314\",\"0.21649\",\"0.23748\",\"0.23668\",\"\",\"\",\"0.23402\",\"0.27195\",\"0.27685\",\"\",\"0.34187\",\"0.16668\",\"0.15703\",\"0.13152\",\"0.11281\",\"0.08964\",\"0.06933\",\"0.068\",\"0.0743\",\"0.06707\",\"\",\"0.09591\",\"0.12041\",\"0.13302\",\"\",\"\",\"\",\"\",\"\",\"0.18688\",\"0.23138\",\"0.27449\",\"0.29156\",\"0.3272\",\"0.33645\",\"0.33047\",\"0.35398\",\"0.36977\",\"0.43901\",\"0.44725\",\"\",\"\",\r\n"));

		//reduceDriver's output
		DoubleWritable y1 = new DoubleWritable(0.33047);
		DoubleWritable y2 = new DoubleWritable(0.44725);
		double dif = y2.get()-y1.get();
		mapReduceDriver.withOutput(new Text("Guinea, tertiary school enrollment\nGPI increase from 2010 to 2014: "), new DoubleWritable((double)Math.round((dif) * 100000d) / 100000d));

		//run() can return a list of key,value pairs for manual testing
		mapReduceDriver.runTest();
	}
}


