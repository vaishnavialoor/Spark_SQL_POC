package com.example.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadFromCSV {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local");

// create Spark Context
        SparkContext context = new SparkContext(conf);

// create spark Session
        SparkSession sparkSession = new SparkSession(context);

        Dataset<Row> df = sparkSession.read().format("com.databricks.spark.csv").option("header", true).option("inferSchema", true).load("C:\\Users\\Admin\\IdeaProjects\\spark_sql_training\\spark-warehouse\\100_emp_Records.csv");

        //("hdfs://localhost:9000/usr/local/hadoop_data/loan_100.csv");
        System.out.println("========== Print Schema ============");
        df.printSchema();
        System.out.println("========== Print Data ==============");
        df.show();
        System.out.println("========== Print title ==============");
        df.select("title").show();
    }
}
