package com.davon;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Flink02_SQL {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.使用SQL方式读取MySQL变化数据
        tableEnv.executeSql("create table spu_sale_attr_value(id int,spu_id int,base_sale_attr_id int,sale_attr_value_name string,sale_attr_name string) with(" +
                "  'connector' = 'mysql-cdc', " +
                "  'hostname' = 'hadoop103', " +
                "  'port' = '3306', " +
                "  'username' = 'root', " +
                "  'password' = '123456', " +
                "  'database-name' = 'gmall_2021', " +
                "  'table-name' = 'spu_sale_attr_value' " +
                ")");

        //3.转换为流打印
        Table table = tableEnv.sqlQuery("select * from spu_sale_attr_value");
        tableEnv.toRetractStream(table, Row.class).print();

        //4.启动
        env.execute();

    }

}
