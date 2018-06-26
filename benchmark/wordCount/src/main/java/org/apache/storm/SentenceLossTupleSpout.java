package org.apache.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.util.TimeUtils;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * locate org.apache.storm
 * Created by mastertj on 2018/6/20.
 * Storm丢失Tuple数量的Benchmark Spout
 */
public class SentenceLossTupleSpout extends BaseRichSpout {
    private static final String TUPLECOUNT_STREAM_ID="tuplecountstream";
    private static final String WORDCOUNT_STREAM_ID="wordcountstream";

    protected SpoutOutputCollector outputCollector;
    protected ConcurrentHashMap<UUID,Values> pending; //用来记录tuple的msgID，和tuple

    private int thisTaskId =0;
    private long tuplecount=0; //记录单位时间Spout发送的元组数量

    private long waitTimeNanos;

    public SentenceLossTupleSpout(long waitTimeNanos) {
        this.waitTimeNanos = waitTimeNanos;
    }

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.outputCollector=spoutOutputCollector;
        this.thisTaskId=topologyContext.getThisTaskId();
        this.pending=new ConcurrentHashMap<UUID, Values>();
        Timer throughputTimer = new Timer();
        //设置计时器没1s计算时间
        throughputTimer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                outputCollector.emit(TUPLECOUNT_STREAM_ID,new Values(tuplecount,System.currentTimeMillis(),thisTaskId));
                tuplecount = 0;
            }
        }, 10,1000);// 设定指定的时间time,此处为1000毫秒
    }

    @Override
    public void nextTuple() {
        String word=randomWords(5);
        //Storm 的消息ack机制
        Values value = new Values(word,System.currentTimeMillis());
        UUID uuid=UUID.randomUUID();
        pending.put(uuid,value);
        outputCollector.emit(WORDCOUNT_STREAM_ID,value,uuid);
        tuplecount++;
        TimeUtils.waitForTimeNanos(waitTimeNanos);
    }

    @Override
    public void ack(Object msgId) {
        pending.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        this.outputCollector.emit(WORDCOUNT_STREAM_ID,pending.get(msgId),msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(TUPLECOUNT_STREAM_ID,new Fields("tuplecount","timeinfo","taskid"));
        outputFieldsDeclarer.declareStream(WORDCOUNT_STREAM_ID,new Fields("word","startTimeMills"));
    }

    /**
     * 随机生成WordCount单词
     * @param wordLength
     * @return
     */
    private String randomWords(int wordLength){
        char[] chars=new char[wordLength];
        for(int i=0;i<wordLength;i++){
            char c=(char)('A'+Math.random()*('Z'-'A'+1));
            chars[i]=c;
        }
        return new String(chars);
    }
}
