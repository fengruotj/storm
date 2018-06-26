package org.apache.storm.report;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.util.DataBaseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Map;

/**
 * Created by 79875 on 2017/3/7.
 * 用来统计输出Spout输入源的吞吐量的Bolt
 */
public class ThroughputReportBolt extends BaseRichBolt {
    private static Logger logger= LoggerFactory.getLogger(ThroughputReportBolt.class);

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector){
        logger.info("------------ThroughputReportBolt prepare------------");
    }

    public void execute(Tuple tuple) {
        Long currentTimeMills = tuple.getLongByField("timeinfo");
        Long tupplecount = tuple.getLongByField("tuplecount");
        int taskid = tuple.getIntegerByField("taskid");

        Timestamp timestamp = new Timestamp(currentTimeMills);

        //将最后结果输出到数据库中
        DataBaseUtil.insertBenchmarkThroughput(taskid, tupplecount, timestamp);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
    }
}
