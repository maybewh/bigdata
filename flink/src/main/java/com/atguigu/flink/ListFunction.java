package com.atguigu.flink;

import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class ListFunction<T> implements AllWindowFunction<T, List<T>, TimeWindow> {

    private int listfunction = 0;
    @Override
    public void apply(TimeWindow window, Iterable<T> values, Collector<List<T>> out) throws Exception {

        List<T> list = new ArrayList<>();
        for (T vaLue : values) {
            list.add(vaLue);
        }
        System.out.println("listfunction:" + ++listfunction);
        System.out.println("time:" + window.toString());
        out.collect(list);
    }
}
