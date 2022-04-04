package com.example.controller;

import org.springframework.web.bind.annotation.RestController;

import scala.Tuple10;
import scala.Tuple2;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;


@RestController
public class SparkCoreController {
    
    @GetMapping(value="read-text-file")
    public ResponseEntity<?> readTextFile() {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("ProductUserCount");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile("testdata/covidloans.csv");

        String header1 = textFile.first();

        // JavaRDD<String[]> startedStreamRDD = textFile.filter(s -> !s.equalsIgnoreCase(header1)).map(s -> s.split(","));
        JavaRDD<String[]> startedStreamRDD = textFile.map(s -> s.split(","));

        return ResponseEntity.ok("read text file successful");
    }

    @GetMapping(value = "countwords")
    public void countWords() {

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JD Word Counter");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> inputFile = sparkContext.textFile("");
        JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")).iterator());
        JavaPairRDD countData = wordsFromFile.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> (int) x + (int) y);
        countData.saveAsTextFile("CountData");
    }
    
}
