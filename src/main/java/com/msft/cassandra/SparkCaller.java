package com.msft.cassandra;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;

public class SparkCaller {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName( "My spark application");
        conf.set("spark.cassandra.connection.host", "52.163.63.183");
        conf.set("input.consistency.level", "ALL");
        conf.setMaster("local[2]");
        SparkContext sc = new SparkContext(conf);

        JavaRDD<Users>  userRDD = CassandraJavaUtil.javaFunctions(sc).cassandraTable("skippy", "users", CassandraJavaUtil.mapRowTo(Users.class));
        System.out.println(userRDD.take(1).get(0).getFirstname());
    }
}
