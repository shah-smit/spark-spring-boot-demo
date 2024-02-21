package com.sparkdemo.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);

		SparkSession spark = SparkSession.builder()
				.appName("SparkDatasetExample")
				.master("local[*]")
				.getOrCreate();

		// Sample data
		List<Row> rowDataList = Arrays.asList(
				RowFactory.create("1", "2024-02-22", Arrays.asList("opp1", "opp2")),
				RowFactory.create("1", "2024-02-21", Arrays.asList("opp3", "opp2")),
				RowFactory.create("1", "2024-02-21", Arrays.asList("opp3", "opp1")),
				RowFactory.create("1", "2024-02-14", Arrays.asList("opp3", "opp1")),
				RowFactory.create("1", "2024-02-13", Arrays.asList("opp3", "opp1")),
				RowFactory.create("1", "2024-01-11", Arrays.asList("opp3", "opp1")),
				RowFactory.create("2", "2024-02-22", Arrays.asList("opp1", "opp2")),
				RowFactory.create("2", "2024-02-21", Arrays.asList("opp3", "opp2")),
				RowFactory.create("2", "2024-02-21", Arrays.asList("opp3", "opp1")),
				RowFactory.create("2", "2024-02-14", Arrays.asList("opp3", "opp1")),
				RowFactory.create("2", "2024-02-13", Arrays.asList("opp3", "opp1")),
				RowFactory.create("2", "2024-01-11", Arrays.asList("opp3", "opp1"))
		);

		// Define the schema using StructType
		StructType schema = new StructType(new StructField[]{
				DataTypes.createStructField("account_id", DataTypes.StringType, false),
				DataTypes.createStructField("batch_date", DataTypes.StringType, false),
				DataTypes.createStructField("list_of_opp_acct", DataTypes.createArrayType(DataTypes.StringType), false)
		});

		// Create a DataFrame from the sample data and schema
		Dataset<Row> dataset = spark.createDataFrame(rowDataList, schema);

		dataset = dataset.withColumn("diff_days", functions.datediff(functions.current_date(), functions.to_date(dataset.col("batch_date"))));

		dataset.show();

		dataset = dataset.withColumn("list_of_opp_acct_leq_1_day", concat(when(col("diff_days").leq(0), col("list_of_opp_acct")).over(Window.partitionBy(col("account_id")))));

		// Show the initial DataFrame
		System.out.println("Initial DataFrame:");
		dataset.show();

		// Perform any transformations or operations as needed

		// Show the final DataFrame with the desired columns
		Dataset<Row> result = dataset.select("account_id", "batch_date", "list_of_opp_acct");
		System.out.println("Final DataFrame:");
		result.show();
	}

}
