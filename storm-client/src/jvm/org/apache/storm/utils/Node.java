package org.apache.storm.utils;

import java.util.ArrayList;
import java.util.List;

public class Node {
    public Node(int id, double positionX, double positionY) {
        this.positionX = positionX;
        this.positionY = positionY;
        nodeId = id;
        edgeNumbers = 0;
        edges = new ArrayList<Integer>();
    }

    public double getPositionX() {
        return positionX;
    }

    public void setPositionX(double positionX) {
        this.positionX = positionX;
    }

    public double getPositionY() {
        return positionY;
    }

    public void setPositionY(double positionY) {
        this.positionY = positionY;
    }

    public double getEdgeNumbers() {
        return edgeNumbers;
    }

    public void setEdgeNumbers(double edgeNumbers) {
        this.edgeNumbers = edgeNumbers;
    }

    public void addEdge(int edgeId) {
        edgeNumbers++;
        edges.add(edgeId);
    }

    public int getNodeId() {
        return nodeId;
    }

    public List<Integer> getEdges() {
        return edges;
    }

    private int nodeId;
    private double positionX;
    private double positionY;
    private double edgeNumbers;
    private List<Integer> edges;
}
