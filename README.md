# Hadoop-MapReduce-Query-Snippets
Sample snippets of various kinds of MapReduce jobs 
Command to run Program 1:
In Program 1, we need to pass the business.csv file.
1) Log into HDFS machine
2) Run the following commands:
hadoop jar PaloAlto.jar BigData.PaloAlto <location of business.csv> /output
eg:hadoop jar PaloAlto.jar BigData.PaloAlto /axa165030/business.csv /output
3) view the file by running:
	hdfs dfs -cat /output/*
	
Command to run Program 2:
In Program 2, we need to pass the review.csv file.
1) Log into HDFS machine
2) Run the following commands:
hadoop jar TopTenRatedBusiness.jar BigData.TopTenRatedBusiness <location of review.csv> /output
eg:hadoop jar TopTenRatedBusiness.jar BigData.TopTenRatedBusiness /axa165030/review.csv /output
3) view the file by running:
	hdfs dfs -cat /output/*
	
	
Command to run Program 3:
In Program 3, we need to pass both-business.csv and review.csv file.
1) Log into HDFS machine
2) Run the following commands:
hadoop jar TopTenBusinessData.jar BigData.TopTenBusinessData <location of review.csv> <location of business.csv> /output
eg:hadoop jar TopTenBusinessData.jar BigData.TopTenBusinessData /axa165030/review.csv /axa165030/business.csv /output
3) view the file by running:
	hdfs dfs -cat /output/*
	

Command to run Program 4:
In Program 4, we need to pass both-business.csv and review.csv file.
1) Log into HDFS machine
2) Run the following commands:
hadoop jar TopTenBusinessData.jar BigData.TopTenBusinessData <location of review.csv> <location of business.csv> /output
eg:hadoop jar TopTenBusinessData.jar BigData.TopTenBusinessData /axa165030/review.csv hdfs://cshadoop1/axa165030/business.csv /output
3) view the file by running:
	hdfs dfs -cat /output/*
