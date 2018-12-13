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
		mapDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Labor force participation rate, male (% of male population ages 15+) (modeled ILO estimate)\",\"SL.TLF.CACT.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"75.3030014038086\",\"74.6279983520508\",\"74.6780014038086\",\"74.3629989624023\",\"74.1149978637695\",\"74.1480026245117\",\"74.1699981689453\",\"74.2570037841797\",\"74.2649993896484\",\"74.2170028686523\",\"74.177001953125\",\"73.6350021362305\",\"73.1289978027344\",\"72.4560012817383\",\"72.2149963378906\",\"72.181999206543\",\"72.3460006713867\",\"72.1019973754883\",\"71.8830032348633\",\"70.8649978637695\",\"70.0189971923828\",\"69.4130020141602\",\"69.3870010375977\",\"69.0029983520508\",\"68.5429992675781\",\"68.4250030517578\",\"68.2839965820313\",\r\n"));
		//expected output
		DoubleWritable y1 = new DoubleWritable(74.177001953125);
		DoubleWritable y2 = new DoubleWritable(68.2839965820313);
		DoubleWritable[] dw = {y1, y2};
		mapDriver.withOutput(new Text("United States, Labor force participation rate, male (% of male population ages 15+) (modeled ILO estimate), 2000 compared to 2016: "), new DoubleArrayWritable(dw));
		//run the test
		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		//making the mock input
		DoubleWritable y1 = new DoubleWritable(74.177001953125);
		DoubleWritable y2 = new DoubleWritable(68.2839965820313);
		DoubleWritable[] dw = {y1, y2};
		List<DoubleArrayWritable> values = new ArrayList<>();
		values.add(new DoubleArrayWritable(dw));

		//mock input (in place of Context context
		reduceDriver.withInput(new Text("United States, Labor force participation rate, male (% of male population ages 15+) (modeled ILO estimate), 2000 compared to 2016: "), values);
		//expected output
		double dif = (y2.get()-y1.get())*-1;
		reduceDriver.withOutput(new Text("United States, Labor force participation rate, male (% of male population ages 15+) (modeled ILO estimate), 2000 compared to 2016: "), new Text("employment percentage decreased by "+dif));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		//mapDriver's input
		mapReduceDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Labor force participation rate, male (% of male population ages 15+) (modeled ILO estimate)\",\"SL.TLF.CACT.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"75.3030014038086\",\"74.6279983520508\",\"74.6780014038086\",\"74.3629989624023\",\"74.1149978637695\",\"74.1480026245117\",\"74.1699981689453\",\"74.2570037841797\",\"74.2649993896484\",\"74.2170028686523\",\"74.177001953125\",\"73.6350021362305\",\"73.1289978027344\",\"72.4560012817383\",\"72.2149963378906\",\"72.181999206543\",\"72.3460006713867\",\"72.1019973754883\",\"71.8830032348633\",\"70.8649978637695\",\"70.0189971923828\",\"69.4130020141602\",\"69.3870010375977\",\"69.0029983520508\",\"68.5429992675781\",\"68.4250030517578\",\"68.2839965820313\",\r\n"));
		
		//reduceDriver's output
		DoubleWritable y1 = new DoubleWritable(74.177001953125);
		DoubleWritable y2 = new DoubleWritable(68.2839965820313);
		double dif = (y2.get()-y1.get())*-1;
		mapReduceDriver.withOutput(new Text("United States, Labor force participation rate, male (% of male population ages 15+) (modeled ILO estimate), 2000 compared to 2016: "), new Text("employment percentage decreased by "+dif));

		//run() can return a list of key,value pairs for manual testing
		mapReduceDriver.runTest();
	}
}

