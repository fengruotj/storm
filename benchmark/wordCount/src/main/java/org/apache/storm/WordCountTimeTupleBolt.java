package org.apache.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * locate org.apache.storm
 * Created by mastertj on 2018/6/20.
 */
public class WordCountTimeTupleBolt extends BaseRichBolt{
    private static Logger logger= LoggerFactory.getLogger(WordCountTimeTupleBolt.class);

    private Map<String, Long> counts = new HashMap<String, Long>();

    private int taskid;
    private OutputCollector outputCollector;
    private boolean isperpare;
    private static String LATENCYTIME_STREAM_ID="latencystream";

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.outputCollector=collector;
        this.taskid=context.getThisTaskId();
        isperpare=true;
    }

    @Override
    public void execute(Tuple tuple) {
        if(isperpare){
            logger.info("currentTimeMills:"+System.currentTimeMillis());
            isperpare=false;
        }
        String word = tuple.getStringByField("word");
        Long startTime=tuple.getLongByField("startTime");

        Long startSerializingTime=tuple.getStartSerializingTime();
        Long endSerializingTime=tuple.getEndSerializingTime();
        Long startDeserializingTime=tuple.getStartDeserializingTime();
        Long endDeserializingTime=tuple.getEndDeserializingTime();
        Long clientTime=tuple.getClientTime();
        Long serverTime=tuple.getServerTime();
        long communicationTime = tuple.getCommunicationTime();

        //logger.info(startTime+"\t"+startSerializingTime+"\t"+endSerializingTime+"\t"+startDeserializingTime+"\t"+endDeserializingTime);

        Long sendWaitTime=startDeserializingTime-startTime-communicationTime;
        Long receiveWaitTime=System.currentTimeMillis()-endDeserializingTime;
        Long deSerializingTime=endDeserializingTime-startDeserializingTime;
        Long serializingTime=endSerializingTime-startSerializingTime;

        Long beforeTimeNano=System.nanoTime()/1000;

        if (!word.isEmpty()) {
            Long count = counts.get(word);
            if (count == null) {
                count = 0L;
            }
            count++;
            counts.put(word, count);
        }

        outputCollector.ack(tuple);
        Long endTimeNano=System.nanoTime()/1000;
        Long computeTime=endTimeNano-beforeTimeNano;

        Long totalTime=System.currentTimeMillis()-startTime;
        this.outputCollector.emit(LATENCYTIME_STREAM_ID,new Values(taskid,startTime,sendWaitTime,serializingTime,communicationTime,deSerializingTime,receiveWaitTime,computeTime,totalTime));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(LATENCYTIME_STREAM_ID,new Fields("taskid","startTime","sendWaitTime","serializingTime","communicationTime","deSerializingTime","receiveWaitTime","computeTime","totalTime"));
    }


    @Override
    public void cleanup() {
    }
}
