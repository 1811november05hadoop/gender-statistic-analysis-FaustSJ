package com.revature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/*
 * Download the data into ~/LData:
 * 
 * 
 * Inside the Project1 folder (~/Repositories/gender.../Project1)
 * hadoop jar target/Project1.jar com.revature.MainJobDriver HData/Gender_StatsData.csv output1
 * 
 * Remember to rebuild the project (as a maven build, "clean package" goal) every time you make a change.
 * Also, delete or rename the output folder whenever you run the hadoop command.
 * 
 * To view the output:
 * hdfs dfs -cat output1/*
 */


public class MainJobDriver {

	public static void main(String[] args) throws Exception{
		
		//code altered and moved to WordCountRunner
		
		//configuration is optional
		//Configuration without parameters just defaults to normal hadoop configurations
		
		//Requirement 1
//		int exitCode = ToolRunner.run(new Configuration(), new FemaleGradLessThan30(), args);
		//Requirement 2
//		int exitCode = ToolRunner.run(new Configuration(), new AvgIncreaseInUSFemEdFrom2000(), args);
		//Requirement 3
//		int exitCode = ToolRunner.run(new Configuration(), new PercentChangeInMaleEmpFrom2000(), args);
		//Requirement 4
//		int exitCode = ToolRunner.run(new Configuration(), new PercentChangeInFemEmpFrom2000(), args);
		//Requirement 5
		int exitCode = ToolRunner.run(new Configuration(), new GenderParityIndexIncreasedInFiveYears(), args);
		
		System.exit(exitCode);
	}
}
