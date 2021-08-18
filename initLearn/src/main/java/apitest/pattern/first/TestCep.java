package apitest.pattern.first;

import apitest.pattern.beans.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class TestCep {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * 接收source并转化为tuple
         */
        DataStream<Tuple3<String,String,String>> myStream = env.addSource(new MySource()).map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                ObjectMapper objectMapper = new ObjectMapper();
                User user = objectMapper.readValue(value, User.class);
                return new Tuple3<>(user.getUserId(), user.getOrderId(), user.getBehave());
            }
        });

        myStream.print("myStream");

        SingleOutputStreamOperator<Tuple3<String, String, String>> reduceStream = myStream.keyBy(0).reduce(new ReduceFunction<Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> reduce(Tuple3<String, String, String> value1, Tuple3<String, String, String> value2) throws Exception {
                String s1 = value1.f1 + value2.f1;
                String s2 = value1.f2 + value2.f2;
                value1.f1 = s1;
                value1.f2 = s2;
                return value1;
            }
        });

        reduceStream.print("reduce");

        /**
         * 定义一个规则：
         * 接受到behave是order后，下一个动作必须是pay才算符合这个需求
         */
        Pattern<Tuple3<String, String, String>, Tuple3<String, String, String>> myPattern = Pattern.<Tuple3<String, String, String>>begin("start")
                .where(new IterativeCondition<Tuple3<String, String, String>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, String> value, Context<Tuple3<String, String, String>> ctx) throws Exception {
                        System.out.println("value:" + value);
                        return value.f2.equals("order");
                    }
                }).next("middle").where(new IterativeCondition<Tuple3<String, String, String>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, String> value, Context<Tuple3<String, String, String>> ctx) throws Exception {
                        System.out.println("value:" + value);
                        return value.f2.equals("pay");
                    }
                }).within(Time.seconds(3));

        DataStream<Tuple3<String, String, String>> key = myStream.keyBy(0);
        key.print("keyBy");
        PatternStream<Tuple3<String, String, String>> pattern = CEP.pattern(key, myPattern);

        // 记录超时的订单
        OutputTag<String> outputTag = new OutputTag<String>("myOutput"){};

        SingleOutputStreamOperator<String> resultStream = pattern.select(outputTag,
                /**
                 * 超时的
                 */
                new PatternTimeoutFunction<Tuple3<String, String, String>, String>() {
                    @Override
                    public String timeout(Map<String, List<Tuple3<String, String, String>>> pattern, long timeoutTimestamp) throws Exception {
                        System.out.println("pattern:" + pattern);
                        List<Tuple3<String, String, String>> startList = pattern.get("start");
                        Tuple3<String, String, String> tuple3 = startList.get(0);
                        return tuple3.toString() + "迟到的";
                    }
                },
                new PatternSelectFunction<Tuple3<String, String, String>, String>() {
                    @Override
                    public String select(Map<String, List<Tuple3<String, String, String>>> pattern) throws Exception {
                        // 匹配上第一个条件的
                        List<Tuple3<String,String,String>> startList = pattern.get("start");
                        // 匹配上第二个条件的
                        List<Tuple3<String, String, String>> endList = pattern.get("middle");

                        Tuple3<String,String,String> tuple3 = endList.get(0);
                        System.out.println("endList:" + endList.size());
                        return tuple3.toString();
                    }
                }
        );

        // 输出匹配上规则的数据
        resultStream.print("result");

        // 输出超时的数据流
        DataStream<String> sideOutput = resultStream.getSideOutput(outputTag);
        sideOutput.print("timeout");

        env.execute();
    }
}
