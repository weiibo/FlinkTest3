package com.liu.chapet09;

import com.liu.chapet05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.time.Duration;

public class TwoSteramJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple3<String,String,Long>> source = env.fromElements(new Tuple3("a", "stream_1", 100l),
                new Tuple3("b", "stream_1", 200l),
                new Tuple3("c", "stream_1", 300l));
        SingleOutputStreamOperator<Tuple3<String, String, Long>> tuple3SingleOutputStreamOperator = source.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> stringStringLongTuple3, long l) {
                        return stringStringLongTuple3.f2;
                    }
                }));
        tuple3SingleOutputStreamOperator.keyBy(data->data.f0).print();
        env.execute();

    }
}