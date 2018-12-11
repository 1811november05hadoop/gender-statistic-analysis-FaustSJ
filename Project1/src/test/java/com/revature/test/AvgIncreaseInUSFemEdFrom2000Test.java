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
import com.revature.map.USFemEducationSince2000Mapper;
import com.revature.models.DoubleArrayWritable;
import com.revature.reduce.AverageReducer;
import com.revature.reduce.LessThan30Reducer;
import com.revature.reduce.YearlyDifferenceReducer;

public class AvgIncreaseInUSFemEdFrom2000Test {
	//with mapper generics
	private MapDriver<LongWritable, Text, Text, DoubleArrayWritable> mapDriver;
	//output of mapper and output of mapper
	private ReduceDriver<Text, DoubleArrayWritable, Text, DoubleWritable> reduceDriver;
	private ReduceDriver<Text, DoubleArrayWritable, Text, DoubleArrayWritable> combineDriver;
	//mrunit mapreduce
	//input of mapper, output of mapper which happens to be input of reducer
	//		and then out put of the reducer
	private MapReduceDriver<LongWritable, Text, Text, DoubleArrayWritable, Text, DoubleWritable> mapReduceDriver;

	@Before
	public void setUp() {
		USFemEducationSince2000Mapper mapper = new USFemEducationSince2000Mapper();
		mapDriver = new MapDriver<>(); //types are inferred
		mapDriver.setMapper(mapper);
		
		YearlyDifferenceReducer combiner = new YearlyDifferenceReducer();
		
		combineDriver = new ReduceDriver<>();
		combineDriver.setReducer(combiner);
		
		AverageReducer reducer = new AverageReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setCombiner(combiner);
		mapReduceDriver.setReducer(reducer);
	}

	//with input, with output
	//insertions are built behind the scenes
	@Test
	public void testMapper() {
		//giving it a psudo-file line
		mapDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"School enrollment, secondary, female (% gross)\",\"SE.SEC.ENRR.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"60.67222\",\"\",\"\",\"\",\"\",\"\",\"85.3929\",\"\",\"\",\"91.41668\",\"94.67477\",\"94.75381\",\"96.46224\",\"95.0773\",\"96.60813\",\"96.64305\",\"\",\"\",\"91.47174\",\"92.25663\",\"\",\"95.98984\",\"96.81125\",\"96.40682\",\"95.25981\",\"\",\"96.92163\",\"\",\"93.88283\",\"94.77221\",\"93.0587\",\"95.3905\",\"96.89634\",\"97.06396\",\"95.63982\",\"96.82829\",\"96.10534\",\"96.43099\",\"95.16566\",\"95.25471\",\"95.58667\",\"96.37354\",\"98.47282\",\"\",\"\",\r\n"));
		//expected output
		double[] doub = {93.88283,94.77221,93.0587,95.3905,96.89634,97.06396,95.63982,96.82829,96.10534,96.43099,95.16566,95.25471,95.58667,96.37354,98.47282};
		DoubleWritable[] dw = new DoubleWritable[doub.length];
		for(int i=0; i<doub.length;i++) {
			dw[i] = new DoubleWritable(doub[i]);
		}
		mapDriver.withOutput(new Text("United States, School enrollment, secondary, female (% gross) (2000 through 2014): "), new DoubleArrayWritable(dw));
		//run the test
		mapDriver.runTest();
	}

	@Test
	public void testCombiner() {
		//make the DoubleArrayWritable mock input
		double[] doub = {93.88283,94.77221,93.0587,95.3905,96.89634,97.06396,95.63982,96.82829,96.10534,96.43099,95.16566,95.25471,95.58667,96.37354,98.47282};
		DoubleWritable[] dw = new DoubleWritable[doub.length];
		for(int i=0; i<doub.length;i++) {
			dw[i] = new DoubleWritable(doub[i]);
		}
		List<DoubleArrayWritable> values = new ArrayList<>();
		values.add(new DoubleArrayWritable(dw));
		
		//mock input
		combineDriver.withInput(new Text("United States, School enrollment, secondary, female (% gross) (2000 through 2014): "), values);
		
		//make the DoubleArrayWritable mock output
		double[] dif = {0.88938,-1.71351,2.3318,1.50584,0.16762,-1.42414,1.18847,-0.72295,0.32565,-1.26533,0.08905,0.33196,0.78687,2.09928};
		DoubleWritable[] dwoutput = new DoubleWritable[dif.length];
		for(int i=0; i<dif.length;i++) {
			dwoutput[i] = new DoubleWritable(dif[i]);
		}
		//expected output
		combineDriver.withOutput(new Text("United States, School enrollment, secondary, female (% gross) (2000 through 2014): "), new DoubleArrayWritable(dwoutput));

		combineDriver.runTest();
		
	}
	
	@Test
	public void testReducer() {
		double[] dif = {0.88938,-1.71351,2.3318,1.50584,0.16762,-1.42414,1.18847,-0.72295,0.32565,-1.26533,0.08905,0.33196,0.78687,2.09928};
		DoubleWritable[] dwoutput = new DoubleWritable[dif.length];
		for(int i=0; i<dif.length;i++) {
			dwoutput[i] = new DoubleWritable(dif[i]);
		}
		List<DoubleArrayWritable> values = new ArrayList<>();
		values.add(new DoubleArrayWritable(dwoutput));

		//mock input
		reduceDriver.withInput(new Text("United States, School enrollment, secondary, female (% gross) (2000 through 2014): "), values);
		//expected output
		reduceDriver.withOutput(new Text("United States, School enrollment, secondary, female (% gross) (2000 through 2014): "), new DoubleWritable(4.58999/14.0));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		//mapDriver's input
		mapReduceDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"School enrollment, secondary, female (% gross)\",\"SE.SEC.ENRR.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"60.67222\",\"\",\"\",\"\",\"\",\"\",\"85.3929\",\"\",\"\",\"91.41668\",\"94.67477\",\"94.75381\",\"96.46224\",\"95.0773\",\"96.60813\",\"96.64305\",\"\",\"\",\"91.47174\",\"92.25663\",\"\",\"95.98984\",\"96.81125\",\"96.40682\",\"95.25981\",\"\",\"96.92163\",\"\",\"93.88283\",\"94.77221\",\"93.0587\",\"95.3905\",\"96.89634\",\"97.06396\",\"95.63982\",\"96.82829\",\"96.10534\",\"96.43099\",\"95.16566\",\"95.25471\",\"95.58667\",\"96.37354\",\"98.47282\",\"\",\"\",\r\n"));

		//reduceDriver's output
		mapReduceDriver.withOutput(new Text("United States, School enrollment, secondary, female (% gross) (2000 through 2014): "), new DoubleWritable(4.58999/14.0));

		//run() can return a list of key,value pairs for manual testing
		mapReduceDriver.runTest();
	}
}
