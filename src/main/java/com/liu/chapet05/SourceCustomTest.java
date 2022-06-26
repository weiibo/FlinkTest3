package com.liu.chapet05;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class SourceCustomTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new SourceFunction<Event>() {

            boolean running = true;
            Random random = new Random();
            String [] names = {"aa","bb","cc","dd","ee"};
            String [] urls  = {"/home","/root","boot","admin"};


            @Override
            public void run(SourceContext<Event> sourceContext) throws Exception {
                while (running){
                    sourceContext.collect(new Event(names[random.nextInt(names.length)],urls[random.nextInt(urls.length)], Calendar.getInstance().getTimeInMillis() ));
                    Thread.sleep(1000);
                }

            }

            @Override
            public void cancel() {
                running = false;
            }
        }).print();
        env.execute("event");


    }

}
