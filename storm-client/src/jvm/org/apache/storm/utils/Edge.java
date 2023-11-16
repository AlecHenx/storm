package org.apache.storm.utils;


import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Edge {

    public Edge(int edgeId_, int srcNodeId_, int destNodeId_) {
        this.edgeId_ = edgeId_;
        this.srcNodeId_ = srcNodeId_;
        this.destNodeId_ = destNodeId_;
        this.shape_ = new ArrayList<>();
    }

    public List<Map.Entry<Double, Double>> getShape() {
        return shape_;
    }

    public void addPosition(double x, double y) {
        shape_.add(new AbstractMap.SimpleEntry<>(x, y));
    }

    public int getEdgeId_() {
        return edgeId_;
    }

    public int getSrcNodeId_() {
        return srcNodeId_;
    }

    public int getDestNodeId_() {
        return destNodeId_;
    }

    private int edgeId_;
    private int srcNodeId_;
    private int destNodeId_;
    private List<Map.Entry<Double, Double>> shape_;
}
