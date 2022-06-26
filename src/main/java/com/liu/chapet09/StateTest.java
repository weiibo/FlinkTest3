package com.liu.chapet09;

import com.liu.chapet05.ClickSource;
import com.liu.chapet05.Event;
import javafx.application.Application;
import javafx.stage.Stage;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class StateTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> source = env.addSource(new ClickSource());

        SingleOutputStreamOperator<Event> stream = source.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofMillis(0))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestap;
                    }
                }));
        stream.keyBy(data->data.name).flatMap(new MyFlatmap()).print();
        env.execute();


    }

    public static class MyFlatmap extends RichFlatMapFunction<Event,String>{
        private ValueState<String> eventValueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            eventValueState = getRuntimeContext().
                    getState(new ValueStateDescriptor<String>("MY STATE",String.class));
        }

        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            System.out.println(eventValueState.value());
            eventValueState.update(event.name);
            System.out.println(eventValueState.value());

        }
    }
}
