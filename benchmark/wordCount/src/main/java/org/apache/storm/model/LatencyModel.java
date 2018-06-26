package org.apache.storm.model;

import java.sql.Timestamp;

/**
 * locate org.apache.storm.model
 * Created by mastertj on 2018/3/7.
 */
public class LatencyModel {
    private Timestamp timestamp;
    private int taskid;
    private Long latency;

    public LatencyModel() {
    }

    public LatencyModel(Timestamp timestamp, int taskid, Long latency) {
        this.timestamp = timestamp;
        this.taskid = taskid;
        this.latency = latency;
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

    public Long getLatency() {
        return latency;
    }

    public void setLatency(Long latency) {
        this.latency = latency;
    }

    @Override
    public String toString() {
        return "LatencyModel{" +
                "timestamp=" + timestamp +
                ", taskid=" + taskid +
                ", latency=" + latency +
                '}';
    }
}
