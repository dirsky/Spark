package com.frank.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.Arrays;

/**
 * @author: Guozhong Xu
 * @date: Create in 16:06 2019/8/13
 */
public class RDD2DataFrameWithJson {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("jsonRDD");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> nameRDD = sc.parallelize(Arrays.asList(
                "{\"name\":\"zhangsan\",\"age\":\"18\"}",
                "{\"name\":\"lisi\",\"age\":\"19\"}",
                "{\"name\":\"wangwu\",\"age\":\"20\"}"
        ));
        JavaRDD<String> scoreRDD = sc.parallelize(Arrays.asList(
                "{\"name\":\"zhangsan\",\"score\":\"100\"}",
                "{\"name\":\"lisi\",\"score\":\"200\"}",
                "{\"name\":\"wangwu\",\"score\":\"300\"}"
        ));

        DataFrame namedf = sqlContext.read().json(nameRDD);
        DataFrame scoredf = sqlContext.read().json(scoreRDD);
        namedf.registerTempTable("name");
        scoredf.registerTempTable("score");

        DataFrame result = sqlContext.sql("select name.name,name.age,score.score from name,score where name.name = score.name");
        result.show();

        sc.stop();

    }
}
