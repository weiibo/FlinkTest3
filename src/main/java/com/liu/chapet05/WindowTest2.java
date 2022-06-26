package com.liu.chapet05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.runtime.operators.window.CountWindow;

import java.time.Duration;

public class WindowTest2 {
    /**
     * 窗口分类：
     *  1.按键分区窗口（keyby之后开窗）
     *  stream.keyby(...).window(...)
     *  2.非按键分区窗口
     *  stream.windowAll(...)强制并行度为1
     *  3.窗口API调用
     *  stream.keyby(...)
     *  .window(window assigner)()   //窗口分配器
     *      时间窗口： {event，process}滑动（窗口长度，offset） 滚动（窗口长度，滑动步长，offset）
     *      计数窗口： {event，process}滑动（窗口长度，offset） 滚动（窗口长度，滑动步长，offset）
     *      会话窗口 ：{event，process}
     *  .aggregate(window function)  //窗口函数
     *      1.增量聚合函数
     *          {min，max，sum...}
     *          ReduceFunction 归约函数
     *          aggregateFunction
     *      2.全窗口函数
     *  4.窗口分配器
     *      指定窗口类型
     */


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Event> source = env.addSource(new ClickSource());
        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = source.assignTimestampsAndWatermarks(WatermarkStrategy.
                <Event>forBoundedOutOfOrderness(Duration.ofMillis(2)).
                withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestap;
                    }
                }));
        eventSingleOutputStreamOperator.map(new MapFunction<Event, Tuple2<String,Long>>() {
            @Override
            public Tuple2<String, Long> map(Event event) throws Exception {
                return Tuple2.of(event.name,1L);
            }
        })
                .keyBy(data->data.f0)
//                    .countWindow(10,2);//计数窗口（个数，步长）
//                .window(EventTimeSessionWindows.withGap(Time.hours(1)));

                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                 滚动时间窗口 (窗口大小，offset)
//                .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(1),Time.minutes(1)))
                //滑动时间窗口(窗口大小，滑动步长，offset，)
//        .aggregate()
        .reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> t1) throws Exception {
                return Tuple2.of(stringLongTuple2.f0,stringLongTuple2.f1+t1.f1);
            }
        }).print();
        env.execute();

    }
}
