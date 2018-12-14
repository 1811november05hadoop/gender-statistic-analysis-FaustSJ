package com.revature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/*
 * Download the data into ~/LData:
 * wget fullDownloadURL
 * 		(while inside the LData folder)
 * unzip downloadedFoldersName
 * 		(if it's zipped)
 * 
 * Copy data to hdfs's HData folder:
 * hdfs dfs -copyFromLocal downloadedFile HData
 * 		(while in where the file is located (LData), or type out the filepath to it)
 * 
 * Remember to rebuild the project (as a maven build, "clean package" goal) every time you make a change.
 * Also, delete or rename the output folder whenever you run the hadoop command.
 * 
 * Inside the Project1 folder (~/Repositories/gender.../Project1):
 * hadoop jar target/Project1.jar com.revature.MainJobDriver HData/Gender_StatsData.csv output1 jobNumber
 * 		(where jobNumber is a number 1 through 5 indicating which job to run)
 * 
 * To view the output:
 * hdfs dfs -cat output1/*
 */


public class MainJobDriver {

	public static void main(String[] args) throws Exception{
		
		//code altered and moved to WordCountRunner
		if(args.length != 3) {
			System.err.println("Usage: MainJobDriver <input dir> <output dir> <job number>");
			System.exit(-1);
		}
		
		try{
			Integer.parseInt(args[2]);
		} catch (Exception e){
			System.err.println("Please give a numerical value for the job number.");
			System.exit(-1);
		}
		
		//configuration is optional
		//Configuration without parameters just defaults to normal hadoop configurations
		int exitCode = 0;
		
		if(Integer.parseInt(args[2])==1)
		//Requirement 1
			exitCode = ToolRunner.run(new Configuration(), new FemaleGradLessThan30(), args);
		if(Integer.parseInt(args[2])==2)
		//Requirement 2
			exitCode = ToolRunner.run(new Configuration(), new AvgIncreaseInUSFemEdFrom2000(), args);
		if(Integer.parseInt(args[2])==3)
		//Requirement 3
			exitCode = ToolRunner.run(new Configuration(), new PercentChangeInMaleEmpFrom2000(), args);
		if(Integer.parseInt(args[2])==4)
		//Requirement 4
			exitCode = ToolRunner.run(new Configuration(), new PercentChangeInFemEmpFrom2000(), args);
		if(Integer.parseInt(args[2])==5)
		//Requirement 5
			exitCode = ToolRunner.run(new Configuration(), new GenderParityIndexIncreasedInFiveYears(), args);
		
		System.exit(exitCode);
	}
}
