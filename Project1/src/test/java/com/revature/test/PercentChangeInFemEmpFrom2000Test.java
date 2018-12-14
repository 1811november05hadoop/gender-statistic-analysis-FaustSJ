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

import com.revature.map.GlobalFemEmploymentSince2000Mapper;
import com.revature.models.DoubleArrayWritable;
import com.revature.reduce.DifferenceReducer;

public class PercentChangeInFemEmpFrom2000Test {
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
		GlobalFemEmploymentSince2000Mapper mapper = new GlobalFemEmploymentSince2000Mapper();
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
		mapDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Employment to population ratio, 15+, female (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"52.5610008239746\",\"52.5690002441406\",\"52.9469985961914\",\"54.1669998168945\",\"54.6139984130859\",\"55.0340003967285\",\"55.7900009155273\",\"56.1259994506836\",\"56.4620018005371\",\"56.6199989318848\",\"56.1189994812012\",\"55.2980003356934\",\"55.1599998474121\",\"55.0439987182617\",\"55.3009986877441\",\"55.7270011901855\",\"55.7519989013672\",\"55.3969993591309\",\"53.5410003662109\",\"52.6469993591309\",\"52.1679992675781\",\"52.2569999694824\",\"52.3470001220703\",\"52.693000793457\",\"53.1230010986328\",\"53.2099990844727\",\r\n"));
		//expected output
		DoubleWritable y1 = new DoubleWritable(56.6199989318848);
		DoubleWritable y2 = new DoubleWritable(53.2099990844727);
		DoubleWritable[] dw = {y1, y2};
		mapDriver.withOutput(new Text("United States, change in the % of females employed,\n\t2000 compared to 2016: "), new DoubleArrayWritable(dw));
		//run the test
		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		//making the mock input
		DoubleWritable y1 = new DoubleWritable(56.6199989318848);
		DoubleWritable y2 = new DoubleWritable(53.2099990844727);
		DoubleWritable[] dw = {y1, y2};
		List<DoubleArrayWritable> values = new ArrayList<>();
		values.add(new DoubleArrayWritable(dw));

		//mock input (in place of Context context
		reduceDriver.withInput(new Text("United States, change in the % of females employed,\n\t2000 compared to 2016: "), values);
		//expected output
		double dif = (double)Math.round(((y2.get()-y1.get())*-1) * 100000d) / 100000d;
		reduceDriver.withOutput(new Text("United States, change in the % of females employed,\n\t2000 compared to 2016: "), new Text("employment percentage decreased by "+dif));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		//mapDriver's input
		mapReduceDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Employment to population ratio, 15+, female (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"52.5610008239746\",\"52.5690002441406\",\"52.9469985961914\",\"54.1669998168945\",\"54.6139984130859\",\"55.0340003967285\",\"55.7900009155273\",\"56.1259994506836\",\"56.4620018005371\",\"56.6199989318848\",\"56.1189994812012\",\"55.2980003356934\",\"55.1599998474121\",\"55.0439987182617\",\"55.3009986877441\",\"55.7270011901855\",\"55.7519989013672\",\"55.3969993591309\",\"53.5410003662109\",\"52.6469993591309\",\"52.1679992675781\",\"52.2569999694824\",\"52.3470001220703\",\"52.693000793457\",\"53.1230010986328\",\"53.2099990844727\",\r\n"));

		//reduceDriver's output
		DoubleWritable y1 = new DoubleWritable(56.6199989318848);
		DoubleWritable y2 = new DoubleWritable(53.2099990844727);
		double dif = (double)Math.round(((y2.get()-y1.get())*-1) * 100000d) / 100000d;
		mapReduceDriver.withOutput(new Text("United States, change in the % of females employed,\n\t2000 compared to 2016: "), new Text("employment percentage decreased by "+dif));

		//run() can return a list of key,value pairs for manual testing
		mapReduceDriver.runTest();
	}
}

