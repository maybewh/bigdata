package com.atguigu.network.analysis;

import com.atguigu.network.analysis.beans.PageViewCount;
import com.atguigu.network.analysis.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.Random;

/**
 *  需求：从web服务器的日志中，统计实时的热门访问页面
 */
public class PageView {

    public static void main(String[] args) throws Exception {

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 读取数据，创建DataStream
        URL resource = PageView.class.getResource("/UserBehavior.csv");
        System.out.println(resource.getPath());
        DataStream<String> inputStream = env.readTextFile(resource.getPath());

        // 3.转换为POJO对象
        DataStream<UserBehavior> dataStream = inputStream
                .map(line -> {
                    String[] fields = line.split(",");
                    return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        // 4. 分组开窗聚合，得到每个窗口内各个商品的count值
        SingleOutputStreamOperator<Tuple2<String, Long>> pvResultStream0 = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                    /**
                     * 这样会导致数据全部集中在同一个分区.
                     * 一般来说，一个keyBy是基于hashcode之后进行分区,而此处的key全部为pv，则就导致数据都在一个分区进行计算，
                     * 并没有进行并行计算
                     * @param value
                     * @return
                     * @throws Exception
                     */
                    @Override
                    public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                        return new Tuple2<>("pv", 1L);
                    }
                })
                .keyBy(tuple -> tuple.f0)
                .sum(1);

        // 并行任务改进 设计随机key，解决数据倾斜的问题
        // 但此时就有10种key的每个窗口的聚合值
        SingleOutputStreamOperator<PageViewCount> pvStream =
                dataStream.filter(line -> "pv".equals(line.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> map(UserBehavior value) throws Exception {
                        Random random = new Random();
                        return new Tuple2<Integer, Long>(random.nextInt(10), 1L);
                    }
                })
                .keyBy(tuple -> tuple.f0)
                .timeWindow(Time.hours(1))
                .aggregate(new PvCountAgg(), new PvCountResult());

        // 在上面的基础上，统计同一窗口的聚合值
        DataStream<PageViewCount> pvStreamCount = pvStream
                .keyBy(PageViewCount::getWindowEnd)
                .process(new TotalPvCount());

        pvStreamCount.print();


        env.execute("页面访问统计");
    }


    // 定义聚合函数
    public static class PvCountAgg implements AggregateFunction<Tuple2<Integer, Long>, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Integer, Long> value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    // 实现自定义窗口
    public static class PvCountResult implements WindowFunction<Long, PageViewCount, Integer, TimeWindow> {

        @Override
        public void apply(Integer integer, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            out.collect(new PageViewCount(integer.toString(), window.getEnd(), input.iterator().next()));
        }
    }

    // 实现同一窗口聚合
    public static class TotalPvCount extends KeyedProcessFunction<Long, PageViewCount, PageViewCount> {

        // 定义状态，保存值
        ValueState<Long> totalCountState;
        ValueState<Long> windowEnd;

        /**
         * 用于初始化 定义的状态
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            totalCountState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("total-count", Long.class, 0L));
            windowEnd = getRuntimeContext().getState(new ValueStateDescriptor<Long>("window-end", Long.class, 0L));
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<PageViewCount> out) throws Exception {
            totalCountState.update(value.getCount() + totalCountState.value()); //聚合相加
            // 注册事件触发器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1); //也就是 当前是窗口结束后的1ms，调用onTimer()方法
            windowEnd.update(value.getWindowEnd());
        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
            // 定时器触发，所有分组count值都到齐，直接输出当前的总count数量
            Long totalCount = totalCountState.value();
            out.collect(new PageViewCount("pv", ctx.getCurrentKey(), totalCount));
            System.out.println("timestamp:" + timestamp);
            System.out.println("ctx currentKey:" + ctx.getCurrentKey());
            System.out.println("windowEnd:" + windowEnd.value());
            // 清空状态
            totalCountState.clear();
        }
    }
    

}
