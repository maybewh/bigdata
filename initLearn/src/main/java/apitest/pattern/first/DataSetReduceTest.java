package apitest.pattern.first;

import apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.File;

public class DataSetReduceTest {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env =  ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        String path = DataSetReduceTest.class.getResource("/").getPath() + File.separator + "sensor.txt";
        DataSource<String> dataSet = env.readTextFile(path);
        DataSet<Tuple2<String, SensorReading>> dataSet1 = dataSet.map(new MapFunction<String, Tuple2<String, SensorReading>>() {
            @Override
            public Tuple2<String, SensorReading> map(String value) throws Exception {
                String[] ss = value.split(",");
                SensorReading sensorReading = new SensorReading(ss[0],Long.parseLong(ss[1]), new Double(ss[3]));
                return new Tuple2<>(ss[0], sensorReading);
            }
        });

        DataSet<Tuple2<String, SensorReading>> dataSet2 = dataSet1.groupBy(0).reduce(new ReduceFunction<Tuple2<String, SensorReading>>() {
            @Override
            public Tuple2<String, SensorReading> reduce(Tuple2<String, SensorReading> value1, Tuple2<String, SensorReading> value2) throws Exception {
                SensorReading sensorReading1 = value1.f1;
                SensorReading sensorReading2 = value2.f1;
                Double tmp = sensorReading1.getTemperature() + sensorReading2.getTemperature();
                sensorReading1.setTemperature(tmp);
                return new Tuple2<>(value1.f0, sensorReading1);
            }
        });

        dataSet2.print();
        env.execute();

    }
}
