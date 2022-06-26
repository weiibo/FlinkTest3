package com.liu.chapet05;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.scala.DataStream;

import java.time.Duration;

/**
 *assignTimestampsAndWatermarks（WatermarkStrategy.
 * <泛型></>forBoundedOutOfOrderness(Duration.ofMillis(2)).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
 *             @Override
 *             public long extractTimestamp(Event event, long l) {
 *                return event.timestap;
 * 传递机制：上游最小水位线分区广播
 * 1.调用WatermarkStrategy内的forBoundedOutOfOrderness/withTimestampAssigner方法
 * 2.new WatermarkStrategy 重写 createWatermarkGenerator方法 return new WatermarkGenerator<Event>()重写onEvent和 和  createTimestampAssigner return new SerializableTimestampAssigner<Event>()
 */


public class WindowTest1 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Event> source = env.addSource(new ClickSource());
    source.assignTimestampsAndWatermarks(WatermarkStrategy.
                <Event>forBoundedOutOfOrderness(Duration.ofMillis(2)).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event event, long l) {
               return event.timestap;
           }
       }));
        source.assignTimestampsAndWatermarks(WatermarkStrategy.
                <Event>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event event, long l) {
                return event.timestap;
            }
        }));
//        source.assignTimestampsAndWatermarks(new WatermarkStrategy<Event>() {
//            @Override
//            public TimestampAssigner <Event> createTimestampAssigner(org.apache.flink.api.common.eventtime.TimestampAssignerSupplier.Context context) {
//                return new SerializableTimestampAssigner<Event>() {
//                    @Override
//                    public long extractTimestamp(Event event, long l) {
//                        return event.timestap;
//                    }
//                };
//            }
//
//            @Override
//            public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
//                return new WatermarkGenerator<Event>() {
//                    private Long delyTime = 5000L;
//                    private Long maxTs = -Long.MAX_VALUE + delyTime +1l;
//                    @Override
//                    public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {
//                        maxTs = Math.max(event.timestap,maxTs);
//                    }
//
//                    @Override
//                    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
//                        watermarkOutput.emitWatermark(new Watermark(maxTs-delyTime-1l));
//
//                    }
//                };
//            }
//        });
        source.assignTimestampsAndWatermarks(new WatermarkStrategy<Event>() {
            @Override
            public TimestampAssigner <Event> createTimestampAssigner(org.apache.flink.api.common.eventtime.TimestampAssignerSupplier.Context context) {
                return new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestap;
                    }
                };
            }

            @Override
            public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<Event>() {
                    private Long delyTime = 5000L;
                    private Long maxTs = -Long.MAX_VALUE + delyTime +1l;
                    @Override
                    public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {
                        maxTs = Math.max(event.timestap,maxTs);
                        if (event.name == "aa"){
                            watermarkOutput.emitWatermark(new Watermark(maxTs-delyTime-1l));
                        }
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
//                        watermarkOutput.emitWatermark(new Watermark(maxTs-delyTime-1l));

                    }
                };
            }
        });

    }
}



