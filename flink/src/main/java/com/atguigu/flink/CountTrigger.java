package com.atguigu.flink;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class CountTrigger extends Trigger<Object, TimeWindow> {

    private int count = 0;

    private int triggerCount = 0;

    public CountTrigger(int triggerCount) {
        this.triggerCount = triggerCount;
    }

    /**
     * 当元素大于等于10时，触发一次
     * @param element
     * @param timestamp
     * @param window
     * @param ctx
     * @return
     * @throws Exception
     */
    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.println("onElement execute! count = " + count);
        System.out.println(window.toString() +":" + window.maxTimestamp());
//        ctx.registerEventTimeTimer(window.maxTimestamp()); // 全窗口的最大时间戳

        ctx.registerProcessingTimeTimer(window.maxTimestamp());
        count++;
        if (count >= triggerCount) {
            count = 0;
//            ctx.deleteProcessingTimeTimer(window.maxTimestamp()); // todo 为什么删除这个
            ctx.deleteProcessingTimeTimer(window.maxTimestamp());
            System.out.println("元素到了指定数量，触发");
            return TriggerResult.FIRE_AND_PURGE; // 触发并清空该窗口中的元素
        }
        return TriggerResult.CONTINUE;

    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.println("onProcessingTime触发了");
        System.out.println("count:" + count);

        if (time >= window.maxTimestamp() && count > 0) { // 时间到了，并且窗口中有值
            count = 0;
            System.out.println("时间到了，触发");
            return TriggerResult.FIRE_AND_PURGE;
        } else if (time >= window.maxTimestamp() && count == 0) {
            return TriggerResult.PURGE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (count > 0) {
            count = 0;
            System.out.println("onEvent触发");
            System.out.println("执行计算，并清空窗口中的值");
            return TriggerResult.FIRE_AND_PURGE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.println("clear()方法触发了");
        System.out.println("clear maxTimestamp：" + window.maxTimestamp());
        ctx.deleteProcessingTimeTimer(window.maxTimestamp()); // Delete the processing time trigger for the given time.
    }
}
