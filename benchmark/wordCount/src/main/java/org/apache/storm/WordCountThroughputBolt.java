package org.apache.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


/**
 * locate com.basic.storm.bolt
 * Created by tj on 2017/5/8.
 */
public class WordCountThroughputBolt extends BaseRichBolt {
    private static Logger logger= LoggerFactory.getLogger(WordCountThroughputBolt.class);

    private Map<String, Long> counts = new HashMap<String, Long>();

    private int taskid;
    private OutputCollector outputCollector;
    private boolean isperpare;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.outputCollector=collector;
        isperpare=true;
    }

    @Override
    public void execute(Tuple tuple) {
        if(isperpare){
            logger.info("currentTimeMills:"+System.currentTimeMillis());
            isperpare=false;
        }
        String word = tuple.getStringByField("word");
        Long startTimeMills=tuple.getLongByField("startTimeMills");
        if (!word.isEmpty()) {
            Long count = counts.get(word);
            if (count == null) {
                count = 0L;
            }
            count++;
            counts.put(word, count);
        }

        outputCollector.ack(tuple);
        //TimeUtils.waitForTimeMills(10);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declarer.declareStream(LATENCYTIME_STREAM_ID,new Fields("latencytime","timeinfo","taskid"));
    }


    @Override
    public void cleanup() {
    }

}
