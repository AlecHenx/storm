package org.apache.storm.starter.bolt;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Edge;
import org.apache.storm.utils.Graph;
import org.apache.storm.utils.Node;

import java.util.*;

import static java.lang.Math.max;
import static java.lang.Math.min;

// input: ["trajId", "edgeId", "dist"], output: []
public class StreamCompressBolt extends BaseBasicBolt {
    private List<Tuple> buffer_;
    private List<Values> output_;
    private int threshold_;
    private Graph roadmap_;

    class Line {
        public Line(Edge edge) {
            start = roadmap_.getNodeById(edge.getSrcNodeId_()).getX_();
            end = roadmap_.getNodeById(edge.getSrcNodeId_()).getY_();
        }

        public double start;
        public double end;
    }

    private double computeAngle(Line line, Line anotherLine) {
        // 在这里实现计算角度的逻辑
        // 返回角度值
        // TODO: implementation
        return 1.0;
    }

    // FIXME(hch): 实际上我只需要把prev的所有边（其中包括了cur相关的这条边），全部转移到prevprev上。
    //  只借助prevprev的一个方向而已，其实没有借助prevprev自己的边。
    private int numDirection(Line line, List<Integer> edgeList, int cur) {
        List<Map.Entry<Integer, Double>> res = new ArrayList<>();
        double angle;

        for (Integer edgeId : edgeList) {
            int startNodeId = roadmap_.getEdgeById(edgeId).getSrcNodeId_();
            int theOtherNodeId = roadmap_.getEdgeById(edgeId).getDestNodeId_();

            Node theOtherNode = roadmap_.getNodeById(theOtherNodeId);
            angle = computeAngle(line, new Line(roadmap_.getEdgeById(edgeId)));
            res.add(Map.entry(theOtherNodeId, angle));
        }

        Collections.sort(res, Comparator.comparing(Map.Entry::getValue));

        for (int i = 0; i < res.size(); i++) {
            Map.Entry<Integer, Double> entry = res.get(i);
            if (entry.getKey() == cur) return i;
        }
        return -1;
    }

    private int calBitWidth(int num) {
        // TODO: implementation
        return 0;
    }

    private void internalCompress() {
        int step = 1;
        int size = buffer_.size();
        int bitWidth = 0;
        for (int i = 1; i < size; ++i) {
            int prevEdgeId = buffer_.get(i - 1).getInteger(1);
            int curEdgeId = buffer_.get(i).getInteger(1);
            if (prevEdgeId == curEdgeId) {
                output_.add(new Values(buffer_.get(i).getInteger(0),
                        0, 0, buffer_.get(i).getInteger(2)));
                continue;
            }
            int prevNodeId = roadmap_.getEdgeById(prevEdgeId).getSrcNodeId_();
            int middleNodeId = roadmap_.getEdgeById(prevEdgeId).getDestNodeId_();

            int curNodeId = roadmap_.getEdgeById(curEdgeId).getSrcNodeId_();
            int nextNodeId = roadmap_.getEdgeById(curEdgeId).getDestNodeId_();

            int direction = numDirection(new Line(roadmap_.getEdgeById(prevEdgeId)),
                    roadmap_.getNodeById(curNodeId).getEdges_(), nextNodeId);
            int choice = -1;
            assert (direction != -1);
            if (i <= step) {
                bitWidth = max(bitWidth, calBitWidth(direction));
                choice = 0;
            } else {
                prevEdgeId = buffer_.get(i - 1).getInteger(1);
                int backup = numDirection(new Line(roadmap_.getEdgeById(prevEdgeId)),
                        roadmap_.getNodeById(curNodeId).getEdges_(), nextNodeId);

                bitWidth = max(bitWidth, calBitWidth(min(direction, backup)));
                assert (bitWidth <= 2);

                if (direction <= backup) {
                    choice = 0;
                } else {
                    direction = backup;
                    choice = 1;
                }
            }
            output_.add(new Values(buffer_.get(i).getInteger(0),
                    direction, choice, buffer_.get(i).getInteger(2)));
        }

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.buffer_ = new ArrayList<Tuple>();
        this.threshold_ = 100;
        roadmap_ = context.getRoadMap_();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        buffer_.add(input);
        if (buffer_.size() >= threshold_) {
            // produce new data into next operator
            internalCompress();
            for (Values value : output_) {
                // Emit the result to downstream bolts if necessary
                collector.emit(value);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("trajId", "direction", "choice", "dist"));
    }
}

