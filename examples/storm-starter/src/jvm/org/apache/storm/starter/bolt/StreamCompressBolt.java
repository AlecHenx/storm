//package org.apache.storm.starter.bolt;
//
////import java.util.*;
//
//import static java.lang.Math.max;
//import static java.lang.Math.min;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.Comparator;
//import java.util.List;
//import java.util.Map;
//import org.apache.storm.task.TopologyContext;
//import org.apache.storm.topology.BasicOutputCollector;
//import org.apache.storm.topology.OutputFieldsDeclarer;
//import org.apache.storm.topology.base.BaseBasicBolt;
//import org.apache.storm.tuple.Fields;
//import org.apache.storm.tuple.Tuple;
//import org.apache.storm.tuple.Values;
//import org.apache.storm.utils.Edge;
//import org.apache.storm.utils.Graph;
//import org.apache.storm.utils.Node;
//
//
//// input: ["trajId", "edgeId", "dist"], output: []
//public class StreamCompressBolt extends BaseBasicBolt {
//    private List<Tuple> buffer;
//    private List<Values> output;
//    private int threshold;
//    private Graph roadmap;
//
//    class Line {
//        Line(Edge edge) {
//            start = roadmap.getNodeById(edge.getSrcNodeId_()).getX_();
//            end = roadmap.getNodeById(edge.getSrcNodeId_()).getY_();
//        }
//
//        public double start;
//        public double end;
//    }
//
//    private double computeAngle(Line line, Line anotherLine) {
//        // 在这里实现计算角度的逻辑
//        // 返回角度值
//        // TODO: implementation
//        return 1.0;
//    }
//
//    // FIXME(hch): 实际上我只需要把prev的所有边（其中包括了cur相关的这条边），全部转移到prevprev上。
//    //  只借助prevprev的一个方向而已，其实没有借助prevprev自己的边。
//    private int numDirection(Line line, List<Integer> edgeList, int cur) {
//        List<Map.Entry<Integer, Double>> res = new ArrayList<>();
//        double angle;
//
//        for (Integer edgeId : edgeList) {
//            int startNodeId = roadmap.getEdgeById(edgeId).getSrcNodeId_();
//            int theOtherNodeId = roadmap.getEdgeById(edgeId).getDestNodeId_();
//
//            Node theOtherNode = roadmap.getNodeById(theOtherNodeId);
//            angle = computeAngle(line, new Line(roadmap.getEdgeById(edgeId)));
//            res.add(Map.entry(theOtherNodeId, angle));
//        }
//
//        Collections.sort(res, Comparator.comparing(Map.Entry::getValue));
//
//        for (int i = 0; i < res.size(); i++) {
//            Map.Entry<Integer, Double> entry = res.get(i);
//            if (entry.getKey() == cur) {
//                return i;
//            }
//        }
//        return -1;
//    }
//
//    private int calBitWidth(int num) {
//        // TODO: implementation
//        return 0;
//    }
//
//    private void internalCompress() {
//        int step = 1;
//        int size = buffer.size();
//        int bitWidth = 0;
//        for (int i = 1; i < size; ++i) {
//            int prevEdgeId = buffer.get(i - 1).getInteger(1);
//            int curEdgeId = buffer.get(i).getInteger(1);
//            if (prevEdgeId == curEdgeId) {
//                output.add(new Values(buffer.get(i).getInteger(0),
//                    0, 0, buffer.get(i).getInteger(2)));
//                continue;
//            }
//            int prevNodeId = roadmap.getEdgeById(prevEdgeId).getSrcNodeId_();
//            int middleNodeId = roadmap.getEdgeById(prevEdgeId).getDestNodeId_();
//
//            int curNodeId = roadmap.getEdgeById(curEdgeId).getSrcNodeId_();
//            int nextNodeId = roadmap.getEdgeById(curEdgeId).getDestNodeId_();
//
//            int direction = numDirection(new Line(roadmap.getEdgeById(prevEdgeId)),
//                roadmap.getNodeById(curNodeId).getEdges_(), nextNodeId);
//            int choice = -1;
//            assert (direction != -1);
//            if (i <= step) {
//                bitWidth = max(bitWidth, calBitWidth(direction));
//                choice = 0;
//            } else {
//                prevEdgeId = buffer.get(i - 1).getInteger(1);
//                int backup = numDirection(new Line(roadmap.getEdgeById(prevEdgeId)),
//                    roadmap.getNodeById(curNodeId).getEdges_(), nextNodeId);
//
//                bitWidth = max(bitWidth, calBitWidth(min(direction, backup)));
//                assert (bitWidth <= 2);
//
//                if (direction <= backup) {
//                    choice = 0;
//                } else {
//                    direction = backup;
//                    choice = 1;
//                }
//            }
//            output.add(new Values(buffer.get(i).getInteger(0),
//                direction, choice, buffer.get(i).getInteger(2)));
//        }
//
//    }
//
//    @Override
//    public void prepare(Map stormConf, TopologyContext context) {
//        this.buffer = new ArrayList<Tuple>();
//        this.threshold = 100;
//        roadmap = context.getRoadMap_();
//    }
//
//    @Override
//    public void execute(Tuple input, BasicOutputCollector collector) {
//        buffer.add(input);
//        if (buffer.size() >= threshold) {
//            // produce new data into next operator
//            internalCompress();
//            for (Values value : output) {
//                // Emit the result to downstream bolts if necessary
//                collector.emit(value);
//            }
//        }
//    }
//
//    @Override
//    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(new Fields("trajId", "direction", "choice", "dist"));
//    }
//}
//
