package org.apache.storm.model;

import java.sql.Timestamp;

/**
 * locate org.apache.storm.model
 * Created by mastertj on 2018/3/7.
 */
public class ThroughputModel{
    private Timestamp timestamp;
    private int taskid;
    private Long tupplecount;

    public ThroughputModel() {
    }

    public ThroughputModel(Timestamp timestamp, int taskid, Long tupplecount) {
        this.timestamp = timestamp;
        this.taskid = taskid;
        this.tupplecount = tupplecount;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public int getTaskid() {
        return taskid;
    }

    public void setTaskid(int taskid) {
        this.taskid = taskid;
    }

    public Long getTupplecount() {
        return tupplecount;
    }

    public void setTupplecount(Long tupplecount) {
        this.tupplecount = tupplecount;
    }

    @Override
    public String toString() {
        return "ThroughputModel{" +
                "timestamp=" + timestamp +
                ", taskid=" + taskid +
                ", tupplecount=" + tupplecount +
                '}';
    }
}
