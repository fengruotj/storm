package org.apache.storm.report;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Map;

/**
 * Created by 79875 on 2017/3/7.
 * 用来统计输出Spout输入源的延迟的Bolt
 * 计算输出结果第二列和第三列的平均值函数：cat xxx | awk '{sum1+=$3;sum2+=$4;sum3+=$5;sum4+=$6;sum5+=$7;sum6+=$8;count++}END{print sum1/count,sum2/count,sum3/count,sum4/count,sum5/count,sum6/count}'
 */
public class TimeReportBolt extends BaseRichBolt {
    private static Logger LOG= LoggerFactory.getLogger(TimeReportBolt.class);

    private OutputCollector outputCollector;

    private BufferedWriter bufferedWriter;

    public static final String fileName="/home/TJ/benchmark/time.out";

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector){
        try {
            this.outputCollector=outputCollector;
            File file = new File(fileName);
            if(!file.exists()){
                file.createNewFile();
            }
            bufferedWriter=new BufferedWriter(new FileWriter(fileName));
            LOG.info("------------LatencyReportBolt prepare------------");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void execute(Tuple tuple) {
        //将最后结果输出到数据库文件中
        try {
            Integer taskid = tuple.getIntegerByField("taskid");
            Long startTime = tuple.getLongByField("startTime");
            Long sendWaitTime = tuple.getLongByField("sendWaitTime");
            Long serializingTime = tuple.getLongByField("serializingTime");
            Long communicationTime=tuple.getLongByField("communicationTime");
            Long deSerializingTime=tuple.getLongByField("deSerializingTime");
            Long receiveWaitTime=tuple.getLongByField("receiveWaitTime");
            Long computeTime=tuple.getLongByField("computeTime");
            Long totalTime=tuple.getLongByField("totalTime");

            bufferedWriter.write(""+taskid+"\t"+startTime+"\t"+sendWaitTime+"\t"+serializingTime+"\t"+communicationTime+"\t"+deSerializingTime+"\t"+receiveWaitTime+"\t"+computeTime+"\t"+totalTime);
            bufferedWriter.newLine();
            bufferedWriter.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
    }
}
