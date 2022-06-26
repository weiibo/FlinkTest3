package com.liu.chapet02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class BoundeStreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("D:\\Hadooptest\\FlinkTest1\\input\\words.txt");
        stringDataStreamSource.flatMap((String s,Collector<Tuple2<String,Long>> out)->{
            String [] words =  s.split(" ");
            for (String word:words){
                out.collect(Tuple2.of(word,1l));
            }

        } ).returns(Types.TUPLE(Types.STRING,Types.LONG)).keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long>  stringLongTuple2) throws Exception {
                return stringLongTuple2.f0;
            }
        }).sum(1).print();//data ->data.f0
        env.execute();

    }
}
