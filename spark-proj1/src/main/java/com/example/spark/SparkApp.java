package com.example.spark;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class SparkApp {

	public static void main(String[] args) {
		
		SpringApplication.run(SparkApp.class, args);
		
		List<Double> input = new ArrayList<Double>();
		input.add(10.5);
		input.add(12.5);
		input.add(14.5);
		input.add(18.5);
		
//		Logger.getLogger("org.apache").setLevel(Level.WARNING);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext jsc = new JavaSparkContext(conf);
//		
//		JavaRDD<Double> response = jsc.parallelize(input);
//		
//		jsc.close();
	}
}
