package com.revature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;



public class WordCount {

	public static void main(String[] args) throws Exception{
		
		//code altered and moved to WordCountRunner
		
		//configuration is optional
		//Configuration without parameters just defaults to normal hadoop configurations
		int exitCode = ToolRunner.run(new Configuration(), new AvgIncreaseInUSFemEdFrom2000(), args);
//		int exitCode = ToolRunner.run(new Configuration(), new FemaleGradLessThan30(), args);
//		int exitCode = ToolRunner.run(new Configuration(), new PercentChangeInFemEmpFrom2000(), args);
//		int exitCode = ToolRunner.run(new Configuration(), new PercentChangeInMaleEmpFrom2000(), args);

		System.exit(exitCode);
	}
}
