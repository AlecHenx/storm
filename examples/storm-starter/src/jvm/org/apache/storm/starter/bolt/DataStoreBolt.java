package org.apache.storm.starter.bolt;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

// "trajId", "edgeId", "dist"
public class DataStoreBolt extends BaseBasicBolt {
    private PrintWriter writer;
    private Map<String, Object> stormConf;
    private TopologyContext context;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.stormConf = stormConf;
        this.context = context;
        try {
            // Open the file for writing
            // TODO: 之后直接接上rocksdb
//            Path tempDirForTest = Files.createTempDirectory("data-output");
            writer = new PrintWriter(new FileWriter("C:\\Users\\ASUS\\AppData\\Local\\Temp\\storm-data.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Integer trajId = input.getIntegerByField("trajId");
        Long edgeId = input.getLongByField("edgeId");
        Double dist = input.getDoubleByField("dist");

        // Serialize
        writer.println(trajId + ":" + edgeId + ":" + dist);
        writer.flush();
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
