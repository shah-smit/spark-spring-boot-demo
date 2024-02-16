package com.sparkdemo.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class HomeController {
    @GetMapping()
    public void doSomeSpark(){
        SparkSession sparkSession = SparkSession.builder()
                .sparkContext(new JavaSparkContext(new SparkConf().setAppName("Spark Demo").setMaster("local[8]")).sc())
                .getOrCreate();
        log.info("somefin");
    }
}
