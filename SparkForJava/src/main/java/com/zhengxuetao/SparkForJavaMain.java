package com.zhengxuetao;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class SparkForJavaMain {
//    public static void main(String[] args) throws InterruptedException {
//        SparkConf sc = new SparkConf().setAppName("SparkForJava").setMaster("local[2]");
//        JavaSparkContext jsc = new JavaSparkContext(sc);
//        JavaRDD<String> rdd = jsc.parallelize(Arrays.asList("hdfs,10","hadoop,20","hdfs,15"));
//        JavaRDD<Integer> rdd1 = rdd.map(line -> {
//                    String arr[] = line.split(",");
//                    return Integer.parseInt(arr[1]);
//                });
//        int count = rdd1.reduce((x,y) -> x+y);
//        System.out.println(count);
//    }


    public static void main(String[] args) throws InterruptedException {
        SparkConf sc = new SparkConf().setAppName("SparkForJava").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(5));
//        Map<String, Object> kafkaParams = new HashMap<String, Object>();
//        kafkaParams.put("bootstrap.servers","datanode1:9092,datanode2:9092");
//        kafkaParams.put("auto.offset.reset","earliest");
//        kafkaParams.put("group.id","java");
//        kafkaParams.put("key.deserializer", StringDeserializer.class);
//        kafkaParams.put("value.deserializer", StringDeserializer.class);

//        Collection<String> topics = Arrays.asList("flumetest");
//        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,LocationStrategies.PreferConsistent(),ConsumerStrategies.Subscribe(topics,kafkaParams));
        JavaInputDStream<String> stream = jssc.socketTextStream("192.168.1.212",9999);
        JavaDStream<String> words = stream.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);
        wordCounts.print();
//        stream.mapToPair(new PairFunction<ConsumerRecord<String,String>, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(ConsumerRecord<String, String> s) throws Exception {
//                return new Tuple2<String, Integer>(s.value(),1);
//            }
//        });
//        stream.foreachRDD(rdd -> {
//            rdd.foreach(new VoidFunction<ConsumerRecord<String, String>>() {
//                @Override
//                public void call(ConsumerRecord<String, String> record) throws Exception {
//                    System.out.println(record.key());
//                }
//            });
//        });
        //        stream.mapToPair(s -> new Tuple2<String,Integer>(String.valueOf(s), 1));
//        stream.foreachRDD(new VoidFunction() {
//            @Override
//            public void call(Object o) throws Exception {
//
//            }
//        });
        jssc.start();
        jssc.awaitTermination();
    }
}
