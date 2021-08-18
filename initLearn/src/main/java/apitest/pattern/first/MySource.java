package apitest.pattern.first;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;

public class MySource extends RichSourceFunction<String> {


    @Override
    public void run(SourceContext<String> ctx) throws Exception {

        ObjectMapper mapper = new ObjectMapper();

        Map<String,String> map = new LinkedHashMap<>();
        map.put("userId","1");
        map.put("orderId","2222");
        map.put("behave","order");
        String s1 = mapper.writeValueAsString(map);
        ctx.collect(s1);

        Thread.sleep(1000);

        map.put("userId","1");
        map.put("orderId","2222");
        map.put("behave","pay");
        ctx.collect(mapper.writeValueAsString(map));
        Thread.sleep(1000);

        map.put("userId","2");
        map.put("orderId","2223");
        map.put("behave","pay");
        ctx.collect(mapper.writeValueAsString(map));
        Thread.sleep(1000);

        map.put("userId","2");
        map.put("orderId","2224");
        map.put("behave","order");
        ctx.collect(mapper.writeValueAsString(map));
        Thread.sleep(1000);

        map.put("userId","2");
        map.put("orderId","2224");
        map.put("behave","order");
        ctx.collect(mapper.writeValueAsString(map));
        Thread.sleep(1000);

    }

    @Override
    public void cancel() {

    }
}
