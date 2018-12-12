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
		mapDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Labor force participation rate, female (% of female population ages 15+) (modeled ILO estimate)\",\"SL.TLF.CACT.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"56.3470001220703\",\"56.1430015563965\",\"56.5509986877441\",\"56.6609992980957\",\"57.6339988708496\",\"57.8400001525879\",\"58.1749992370605\",\"58.7039985656738\",\"58.8279991149902\",\"58.9930000305176\",\"59.060001373291\",\"58.8349990844727\",\"58.6059989929199\",\"58.4830017089844\",\"58.1829986572266\",\"58.2859992980957\",\"58.4059982299805\",\"58.3489990234375\",\"58.5530014038086\",\"58.2350006103516\",\"57.5820007324219\",\"56.9729995727539\",\"56.7490005493164\",\"56.3209991455078\",\"56.1209983825684\",\"56.007999420166\",\"55.8720016479492\",\r\n"));
		//expected output
		DoubleWritable y1 = new DoubleWritable(59.060001373291);
		DoubleWritable y2 = new DoubleWritable(55.8720016479492);
		DoubleWritable[] dw = {y1, y2};
		mapDriver.withOutput(new Text("United States, Labor force participation rate, female (% of female population ages 15+) (modeled ILO estimate), 2000 compared to 2016: "), new DoubleArrayWritable(dw));
		//run the test
		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		//making the mock input
		DoubleWritable y1 = new DoubleWritable(59.060001373291);
		DoubleWritable y2 = new DoubleWritable(55.8720016479492);
		DoubleWritable[] dw = {y1, y2};
		List<DoubleArrayWritable> values = new ArrayList<>();
		values.add(new DoubleArrayWritable(dw));

		//mock input (in place of Context context
		reduceDriver.withInput(new Text("United States, Labor force participation rate, female (% of female population ages 15+) (modeled ILO estimate), 2000 compared to 2016: "), values);
		//expected output
		double dif = (y2.get()-y1.get())*-1;
		reduceDriver.withOutput(new Text("United States, Labor force participation rate, female (% of female population ages 15+) (modeled ILO estimate), 2000 compared to 2016: "), new Text("employment percentage decreased by "+dif));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		//mapDriver's input
		mapReduceDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Labor force participation rate, female (% of female population ages 15+) (modeled ILO estimate)\",\"SL.TLF.CACT.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"56.3470001220703\",\"56.1430015563965\",\"56.5509986877441\",\"56.6609992980957\",\"57.6339988708496\",\"57.8400001525879\",\"58.1749992370605\",\"58.7039985656738\",\"58.8279991149902\",\"58.9930000305176\",\"59.060001373291\",\"58.8349990844727\",\"58.6059989929199\",\"58.4830017089844\",\"58.1829986572266\",\"58.2859992980957\",\"58.4059982299805\",\"58.3489990234375\",\"58.5530014038086\",\"58.2350006103516\",\"57.5820007324219\",\"56.9729995727539\",\"56.7490005493164\",\"56.3209991455078\",\"56.1209983825684\",\"56.007999420166\",\"55.8720016479492\",\r\n"));

		//reduceDriver's output
		DoubleWritable y1 = new DoubleWritable(59.060001373291);
		DoubleWritable y2 = new DoubleWritable(55.8720016479492);
		double dif = (y2.get()-y1.get())*-1;
		mapReduceDriver.withOutput(new Text("United States, Labor force participation rate, female (% of female population ages 15+) (modeled ILO estimate), 2000 compared to 2016: "), new Text("employment percentage decreased by "+dif));

		//run() can return a list of key,value pairs for manual testing
		mapReduceDriver.runTest();
	}
}

