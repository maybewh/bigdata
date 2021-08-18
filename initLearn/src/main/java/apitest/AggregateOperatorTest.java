package apitest;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import java.io.File;

public class AggregateOperatorTest {

    public static void main(String[] args) throws Exception {

        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 使得任务抢占同一个线程
        env.setParallelism(1);

        String rootPath = AggregateOperatorTest.class.getResource("/").getPath();
        String filePath = rootPath + File.separator + "sensor.txt";

        // 从文件中获取数据输出
        DataSet<String> dataStream = env.readTextFile(filePath);

        DataSet<Tuple4<String, Long, Double,Double>> tuple2DataSet = dataStream.map(new MapFunction<String, Tuple4<String, Long, Double,Double>>() {
            @Override
            public Tuple4<String, Long, Double,Double> map(String value) throws Exception {
                String[] fields = value.split(",");
                return new Tuple4<String, Long, Double,Double>(fields[0], new Long(fields[1]), new Double(fields[2]), new Double(fields[3]));
            }
        });

        AggregateOperator<Tuple4<String, Long, Double,Double>> source = tuple2DataSet.groupBy(0).
                aggregate(Aggregations.SUM, 3);

        source.print();
        env.execute();
    }
}