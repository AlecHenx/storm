package org.apache.storm.starter.bolt;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

public class StoreBolt extends BaseBasicBolt {
    private PrintWriter writer;
    private Map<String, Object> stormConf;
    private TopologyContext context;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.stormConf = stormConf;
        this.context = context;
        try {
            // Open the file for writing
            writer = new PrintWriter(new FileWriter("output.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        this.context.getConf();
        String key = input.getStringByField("key");
        String value = input.getStringByField("value");

        // Write the key-value pair to the file
        writer.println(key + ":" + value);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

    @Override
    public void cleanup() {
        // Close the file when the bolt is being shutdown
        writer.close();
    }
}


public class MyBolt extends BaseBasicBolt {

    private Map<String, Object> stormConf;
    private TopologyContext context;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.stormConf = stormConf;
        this.context = context;

        // 通过 context 获取 Nimbus 中的元数据
        Map<String, Object> topologyConf = context.getStormConf();
        Map<Integer, String> componentToSortedTasks = context.getComponentTasks("spout");
        int numTasks = context.getComponentTasks(context.getThisComponentId()).size();
        int myTaskId = context.getThisTaskId();

        // 在这里可以使用获取到的元数据进行相应的处理
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        // 在这里处理输入的 Tuple
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // 声明输出的字段
    }
}