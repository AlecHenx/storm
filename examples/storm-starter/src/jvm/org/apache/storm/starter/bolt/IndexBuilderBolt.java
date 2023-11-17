package org.apache.storm.starter.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class IndexBuilderBolt extends BaseBasicBolt {
    Map<Integer, ArrayList<Integer>> index = new HashMap<Integer, ArrayList<Integer>>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Integer regionId = tuple.getInteger(0);
        ArrayList<Integer> trajIds = index.get(regionId);
        if (trajIds == null) {
            trajIds = new ArrayList<>();
        }
        trajIds.add(tuple.getInteger(1));
        index.put(regionId, trajIds);

        collector.emit(new Values(regionId, trajIds));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("regionId", "trajectoryIds"));
    }
}
