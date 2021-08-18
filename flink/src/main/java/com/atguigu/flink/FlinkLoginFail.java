package com.atguigu.flink;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FlinkLoginFail {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkLoginFail.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<LoginEvent> loginEventStream = env.fromCollection(Arrays.asList(
                new LoginEvent("小明","192.168.0.1","fail"),
                new LoginEvent("小明","192.168.0.2","fail"),
                new LoginEvent("小王","192.168.10,11","fail"),
                new LoginEvent("小王","192.168.10,12","fail"),
                new LoginEvent("小明","192.168.0.3","fail"),
                new LoginEvent("小明","192.168.0.4","fail"),
                new LoginEvent("小王","192.168.10,10","success")
        ));

        // 在3秒 内重复登录了三次, 则产生告警
        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern.<LoginEvent>
                begin("first")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context context) throws Exception {
                        System.out.println("first: " + loginEvent);
                        return loginEvent.getType().equals("fail");
                    }
                })
                .next("second")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context context) throws Exception {
                        System.out.println("second: " + loginEvent);
                        return loginEvent.getType().equals("fail");
                    }
                })
                .next("three")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context context) throws Exception {
                        System.out.println("three: " + loginEvent);
                        return loginEvent.getType().equals("fail");
                    }
                })
                .within(Time.seconds(3));

        // 根据用户id分组，以便可以锁定用户IP，cep模式匹配
        PatternStream<LoginEvent> patternStream = CEP.pattern(
                loginEventStream.keyBy(LoginEvent::getUserId),
                loginFailPattern);

        // 获取重复登录三次失败的用户信息
        DataStream<String> loginFailDataStream = patternStream.select(
                new PatternSelectFunction<LoginEvent, String>() {
                    @Override
                    public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
                        List<LoginEvent> second = pattern.get("three");
                        return  second.get(0).getUserId() + ", "+ second.get(0).getIp() + ", "+ second.get(0).getType();
                    }
                }
        );

        // 打印告警用户
        loginFailDataStream.print();
        env.execute();
        LOGGER.info("finish");
    }
}
