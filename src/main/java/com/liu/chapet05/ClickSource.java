package com.liu.chapet05;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<Event> {
    private  Boolean running = true;

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        Random random = new Random();
        String[] users = {"aa","bb","cc","dd","ee"};
        String[] urls = {"baidu","alibaba","jd","tencent"};
        while (running){
            sourceContext.
                    collect(new Event(users[random.nextInt(users.length)],
                            urls[random.nextInt(urls.length)],
                            Calendar.getInstance().getTimeInMillis()));
            Thread.sleep(1000);
        }

    }

    @Override
    public void cancel() {
        running = false;
    }
}
