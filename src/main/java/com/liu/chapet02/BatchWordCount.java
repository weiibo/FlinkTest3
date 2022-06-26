package com.liu.chapet02;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import static org.apache.flink.api.java.tuple.Tuple2.*;

public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = env.readTextFile("D:\\Hadooptest\\FlinkTest1\\input\\words.txt");
        FlatMapOperator<String, Tuple2<String, Long>> stringTuple2FlatMapOperator = dataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String s : words) {
                out.collect(of(s, 1l));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        stringTuple2FlatMapOperator.groupBy(0).sum(1).print();

    }

}
