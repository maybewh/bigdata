package apitest.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.net.URL;

public class TableTest3_CommonApi {

    public static void main(String[] args) throws Exception {

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 表的创建：连接外部系统，读取数据
        // 2.1 读取文件
        URL path = TableTest1_Example.class.getResource("/");
        String url = path.getPath() + "sensor.txt";

        tableEnv.connect(new FileSystem().path(url)) // 定义到文件系统的连接
                .withFormat(new Csv()) //定义以csv格式进行数据结构化
                .withSchema(new Schema()
                                .field("id", DataTypes.STRING())
                                .field("timestamp", DataTypes.BIGINT())
                                .field("temp", DataTypes.DOUBLE())
                )  // 定义表结构
                .createTemporaryTable("inputTable");

        Table inputTable = tableEnv.from("inputTable");
        /*inputTable.printSchema();
        tableEnv.toAppendStream(inputTable, Row.class).print();*/


        // 3. 查询转换
        // 3.1 Table API
        // 简单转化
        Table resultTable = inputTable.select("id, temp").filter("id == 'sensor_6'");

        // 聚合统计
        Table aggTable = inputTable.groupBy("id").select("id, id.count as count, temp.avg as avgTemp");

        //3.2 SQL
//        tableEnv.sqlQuery("select id, temp from inputTable where id = 'sensor_6'");
        Table sqlAggTable = tableEnv.sqlQuery("select id, count(id) as cnt, avg(temp) as avgTemp from inputTable group by id");

        // 打印输出
        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toRetractStream(aggTable, Row.class).print("agg");
//        tableEnv.toRetractStream(sqlAggTable, Row.class).print("sqlAgg");

        env.execute();

    }
}
