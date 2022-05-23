package com.example.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.col;

public class sparksql {
    private static final SparkSession sparkSession =
            SparkSession.builder().master("local[*]").appName("Spark2JdbcDs").getOrCreate();
    public static void main(String[] args) {

        Dataset<Row> emp_df = sparkSession.read().
                format("jdbc").
                option("url", "jdbc:mysql://localhost:3306/employee")
                .option("dbtable", "employee")
                .option("user", "root")
                .option("password", "").load();
        Dataset<Row> loc_df = sparkSession.read().
                format("jdbc").
                option("url", "jdbc:mysql://localhost:3306/employee")
                .option("dbtable", "location")
                .option("user", "root")
                .option("password", "").load();

        emp_df.filter("emp_salary>50000").show(false);
        emp_df.filter("emp_join_date> '2021-01-01'").show(false);
        Dataset <Row> emp_loc_join = emp_df.join(loc_df,emp_df.col("emp_loc_id").equalTo(loc_df.col("loc_id")));
        emp_loc_join.filter("loc_name='Mumbai'").show();
    }
}


