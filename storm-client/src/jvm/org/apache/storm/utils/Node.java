package org.apache.storm.utils;

import java.util.ArrayList;
import java.util.List;

public class Node {
    public Node(int id, double x, double y) {
        x_ = x;
        y_ = y;
        nodeId_ = id;
        edgeNumbers_ = 0;
        edges_ = new ArrayList<Integer>();
    }

    public double getX_() {
        return x_;
    }

    public void setX_(double x_) {
        this.x_ = x_;
    }

    public double getY_() {
        return y_;
    }

    public void setY_(double y_) {
        this.y_ = y_;
    }

    public double getEdgeNumbers_() {
        return edgeNumbers_;
    }

    public void setEdgeNumbers_(double edgeNumbers_) {
        this.edgeNumbers_ = edgeNumbers_;
    }

    public void addEdge(int edgeId) {
        edgeNumbers_++;
        edges_.add(edgeId);
    }

    public int getNodeId_() {
        return nodeId_;
    }

    public List<Integer> getEdges_() {
        return edges_;
    }

    private int nodeId_;
    private double x_;
    private double y_;
    private double edgeNumbers_;
    private List<Integer> edges_;
}
