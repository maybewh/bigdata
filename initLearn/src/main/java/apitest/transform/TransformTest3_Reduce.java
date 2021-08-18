package apitest.transform;

import apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest3_Reduce {

    public static void main(String[] args) throws Exception {

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 执行环境并行度设置1
        env.setParallelism(1);

        DataStream<String> dataStream = env.readTextFile("D:\\development\\code\\idea\\flinkLearn\\src\\main\\resources\\sensor.txt");

        DataStream<SensorReading> sensorStream = dataStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 先分组，再聚合
        KeyedStream<SensorReading, String> keyedStream = sensorStream.keyBy(SensorReading::getId);

        // reduce,reduceFunction中reduce()函数的第一个字段为当前的聚合的数据（状态），第二个为最新的数据
        DataStream<SensorReading> resultStream = keyedStream.reduce(
                (curSensor, newSensor) -> new SensorReading(curSensor.getId(),
                        newSensor.getTimestamp(),
                        Math.max(curSensor.getTemperature(), newSensor.getTemperature()))
        );

        resultStream.print("result");

        env.execute();
    }
}
