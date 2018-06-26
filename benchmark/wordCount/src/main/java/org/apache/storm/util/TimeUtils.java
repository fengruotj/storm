package org.apache.storm.util;

/**
 * locate org.apache.storm.utils
 * Created by mastertj on 2018/3/15.
 */
public class TimeUtils {
    /**
     * waitForTimeMills等待时间
     * @param timeMills
     */
    public static void waitForTimeMills(long timeMills){
        if(timeMills!=0) {
            Long startTimeMllls = System.currentTimeMillis();
            while (true) {
                Long endTimeMills = System.currentTimeMillis();
                if ((endTimeMills - startTimeMllls) >= timeMills)
                    break;
            }
        }
    }

    /**
     * waitForTimeMills等待时间
     * @param timeNanos
     */
    public static void waitForTimeNanos(long timeNanos){
        if(timeNanos!=0) {
            Long startTimeNanos = System.nanoTime();
            while (true) {
                Long endTimeNanos = System.nanoTime();
                if ((endTimeNanos - startTimeNanos) >= timeNanos)
                    break;
            }
        }
    }
}
