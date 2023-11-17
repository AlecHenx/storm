package org.apache.storm.starter.bolt;

import java.util.Map;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;


public class MapMatchBolt extends ShellBolt implements IRichBolt {

    public MapMatchBolt() {
        super("/home/hch/anaconda3/envs/rlts/bin/python", "STmatching_distribution_ver.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("trajId", "edgeId", "dist"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
