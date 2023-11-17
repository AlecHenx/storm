package org.apache.storm.utils;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Edge {

    public Edge(int edgeId, int srcNodeId, int destNodeId) {
        this.edgeId = edgeId;
        this.srcNodeId = srcNodeId;
        this.destNodeId = destNodeId;
        this.shape = new ArrayList<>();
    }

    public List<Map.Entry<Double, Double>> getShape() {
        return shape;
    }

    public void addPosition(double x, double y) {
        shape.add(new AbstractMap.SimpleEntry<>(x, y));
    }

    public int getEdgeId() {
        return edgeId;
    }

    public int getSrcNodeId() {
        return srcNodeId;
    }

    public int getDestNodeId() {
        return destNodeId;
    }

    private int edgeId;
    private int srcNodeId;
    private int destNodeId;
    private List<Map.Entry<Double, Double>> shape;
}
