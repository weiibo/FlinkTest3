package com.liu.chapet05;

import org.apache.commons.net.ntp.TimeStamp;

public class Event {
    public String name;
    public String url;
    public Long timestap;

    public Event() {
    }

    public Event(String name, String url, Long timestap) {
        this.name = name;
        this.url = url;
        this.timestap = timestap;
    }



    @Override
    public String toString() {
        return "Event{" +
                "name='" + name + '\'' +
                ", url='" + url + '\'' +
                ", timestap=" + new TimeStamp(timestap) +
                '}';
    }
}
