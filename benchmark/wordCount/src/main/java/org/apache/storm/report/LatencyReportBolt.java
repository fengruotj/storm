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
 * 计算输出结果第二列和第三列的平均值函数：cat xxx | awk '{sum1+=$2;sum2+=$3;sum3+=$4;sum4+=$5;sum5+=$6;sum6+=$7;count++}END{print sum1/count,sum2/count,sum3/count,sum4/count,sum5/count,sum6/count}'
 */
public class LatencyReportBolt extends BaseRichBolt {
    private static Logger LOG= LoggerFactory.getLogger(LatencyReportBolt.class);

    private OutputCollector outputCollector;

    private BufferedWriter bufferedWriter;

    public static final String fileName="/home/TJ/benchmark/latency.out";

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
            Long sendWaitTime = tuple.getLongByField("sendWaitTime");
            Long receiveWaitTime = tuple.getLongByField("receiveWaitTime");
            Long computeTime = tuple.getLongByField("computeTime");
            Long serializingTime=tuple.getLongByField("serializingTime");
            Long deSerializingTime=tuple.getLongByField("deSerializingTime");
            Long communicationTime=tuple.getLongByField("communicationTime");
            Long totalTime=tuple.getLongByField("totalTime");
//            DataBaseUtil.insertBenchmarkLatency(taskid,communicationTime,computeTime);
            bufferedWriter.write(""+taskid+"\t"+sendWaitTime+"\t"+receiveWaitTime+"\t"+computeTime+"\t"+serializingTime+"\t"+deSerializingTime+"\t"+communicationTime+"\t"+totalTime);
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
