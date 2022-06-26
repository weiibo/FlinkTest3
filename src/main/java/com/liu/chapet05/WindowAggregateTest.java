package com.liu.chapet05;

import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.codehaus.jackson.map.deser.EnumDeserializer;

import java.time.Duration;

public class WindowAggregateTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Event> source = env.addSource(new ClickSource());
        SingleOutputStreamOperator<Event> stream = source.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofMillis(0))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestap;
                    }
                }));
        stream.keyBy(data->data.name)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<Event, Tuple2<Long,Integer>, String>() {
                    @Override
                    public Tuple2<Long, Integer> createAccumulator() {
                        return Tuple2.of(0L,0);
                    }

                    @Override
                    public Tuple2<Long, Integer> add(Event event, Tuple2<Long, Integer> longIntegerTuple2) {
                        return Tuple2.of(event.timestap+longIntegerTuple2.f0,createAccumulator().f1+1);
                    }

                    @Override
                    public String getResult(Tuple2<Long, Integer> longIntegerTuple2) {
                        return new TimeStamp(longIntegerTuple2.f0/longIntegerTuple2.f1).toString();
                    }

                    @Override
                    public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> longIntegerTuple2, Tuple2<Long, Integer> acc1) {
                        return Tuple2.of(longIntegerTuple2.f0+acc1.f0,longIntegerTuple2.f1+acc1.f1);
                    }
                }).print();
        env.execute();
    }
}
