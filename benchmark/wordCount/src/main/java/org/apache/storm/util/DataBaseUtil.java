package org.apache.storm.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;


/**
 * Created by 79875 on 2017/1/9.
 */
public class DataBaseUtil {

    private static Logger logger= LoggerFactory.getLogger(DataBaseUtil.class);

    public static Connection conn;//创建用于连接数据库的Connection对象

    /**
     * 插入数据到tupleCount表中
     * @param taskId
     * @param communicationTime
     * @param computeTime
     */
    public static void insertBenchmarkLatency(int taskId , Long communicationTime, Long computeTime){
        try {
            conn=JdbcPool.getConnection();
            PreparedStatement preparedStatement;
            String sql = "INSERT INTO t_systemlatency(taskid,communicationTime,computeTime)"
                    + " VALUES (?,?,?)";  // 插入数据的sql语句
            preparedStatement = conn.prepareStatement(sql);    // 创建用于执行静态sql语句的Statement对象
            preparedStatement.setInt(1,taskId);
            preparedStatement.setLong(2,communicationTime);
            preparedStatement.setLong(3,computeTime);
            int count = preparedStatement.executeUpdate();  // 执行插入操作的sql语句，并返回插入数据的个数
            preparedStatement.close();
            //logger.info("insert into t_tuplecount (time,tuplecount) values"+time+" "+tuplecount);
            JdbcPool.release(conn,preparedStatement,null);
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    /**
     * 插入数据到t_throughput表中
     * @param taskId
     * @param tuplecount
     * @param timeinfo
     */
    public static void insertBenchmarkThroughput(int taskId , long tuplecount, Timestamp timeinfo){
        try {
            conn=JdbcPool.getConnection();
            PreparedStatement preparedStatement;
            String sql = "INSERT INTO t_systemthroughput(taskid,tuplecount,time)"
                    + " VALUES (?,?,?)";  // 插入数据的sql语句
            preparedStatement = conn.prepareStatement(sql);    // 创建用于执行静态sql语句的Statement对象
            preparedStatement.setInt(1,taskId);
            preparedStatement.setLong(2,tuplecount);
            preparedStatement.setTimestamp(3,timeinfo);
            int count = preparedStatement.executeUpdate();  // 执行插入操作的sql语句，并返回插入数据的个数
            preparedStatement.close();
            //logger.info("insert into t_tuplecount (time,tuplecount) values"+time+" "+tuplecount);
            JdbcPool.release(conn,preparedStatement,null);
        }catch (SQLException e){
            e.printStackTrace();
        }
    }
}
