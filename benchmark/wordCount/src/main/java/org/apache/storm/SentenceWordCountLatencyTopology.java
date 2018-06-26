package org.apache.storm;

import org.apache.storm.report.LatencyReportBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * locate org.apache.storm
 * Created by mastertj on 2018/6/21.
 * 提交stormtopology任务
 * storm jar wordCount-2.0.0-SNAPSHOT.jar org.apache.storm.SentenceWordCountLatencyTopology stormwordcount 2 1 1 1 0
 */
public class SentenceWordCountLatencyTopology {
    private static final String SPOUT_LATENCYREPORT_BOLT_ID = "spout-latency-report";
    private static String SENTENCE_SPOUT_ID="sentence-spout";
    private static String COUNT_BOLT_ID="count-bolt";
    private static String WORDCOUNT_STREAM_ID="wordcountstream";
    private static String LATENCYTIME_STREAM_ID="latencystream";

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder=new TopologyBuilder();
        String topologyName=args[0];
        Integer numworkers=Integer.valueOf(args[1]);
        Integer spoutparallelism=Integer.valueOf(args[2]);
        Integer wordcountboltparallelism=Integer.valueOf(args[3]);
        Integer latencyreportboltparallelism=Integer.valueOf(args[4]);
        long waitTimeNanos=Long.valueOf(args[5]);

        SentenceLatencySpout spout=new SentenceLatencySpout(waitTimeNanos);
        WordCountLatencyTupleBolt wordCountLatencyTupleBolt =new WordCountLatencyTupleBolt();

        LatencyReportBolt latencyReportBolt =new LatencyReportBolt();

        builder.setSpout(SENTENCE_SPOUT_ID,spout,spoutparallelism);
        builder.setBolt(COUNT_BOLT_ID, wordCountLatencyTupleBolt,wordcountboltparallelism)
                .fieldsGrouping(SENTENCE_SPOUT_ID,WORDCOUNT_STREAM_ID,new Fields("word"));
        builder.setBolt(SPOUT_LATENCYREPORT_BOLT_ID, latencyReportBolt,latencyreportboltparallelism)
                .shuffleGrouping(COUNT_BOLT_ID,LATENCYTIME_STREAM_ID);

        //Topology配置
        Config config=new Config();
        config.setNumWorkers(numworkers);//设置两个Worker进程 10
        //config.setNumAckers(0);//每个Work进程会运行一个Acker任务，这里将Ack任务设置为0 禁止Ack任务
        config.setTopologyWorkerMaxHeapSize(2048);
        if(args[0].equals("local")){
            Utils.sleep(50*1000);//50s
        }else {
            StormSubmitter.submitTopology(topologyName,config,builder.createTopology());
        }

    }
}
