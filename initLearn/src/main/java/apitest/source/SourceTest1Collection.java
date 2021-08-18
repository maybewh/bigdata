package apitest.source;

import apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class SourceTest1Collection {

    /**
     * 测试flink从集合中取数据
     * @param args
     */
    public static void main(String[] args) throws Exception {

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置env并行度为1，使得整个任务抢占同一个线程执行
        env.setParallelism(1);

        // Source: 从集合Collection中获取数据
        DataStream<SensorReading> dataStream = env.fromCollection(
                Arrays.asList(new SensorReading("sensor_1", 1547718199L, 35.8),
                        new SensorReading("sensor_6", 1547718201L, 15.4),
                        new SensorReading("sensor_7", 1547718202L, 6.7),
                        new SensorReading("sensor_10", 1547718205L, 38.1)
                )
        );

        DataStream<Integer> inStream = env.fromElements(1,2,3,4,5,6,7,8,9);

        // 打印输出，可以指定任务的名称
        dataStream.print("SENSOR");
        inStream.print("INT");

        // 执行
        env.execute("JobName");
    }
}
