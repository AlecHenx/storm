package org.apache.storm.utils;

import org.apache.storm.metrics2.StormMetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class Graph {

    public Graph(int graphId, List<Node> nodes, List<Edge> edges, boolean doubleCheck) {
        graphId_ = graphId;
        internalLoadGraph(nodes, edges);
        doubleCheck_ = doubleCheck;
    }

    public Graph(int graphId, List<Node> nodes, List<Edge> edges) {
        new Graph(graphId, nodes, edges, false);
    }

    public void internalLoadGraph(List<Node> nodes, List<Edge> edges) {
        // init edges
        for (Edge edge : edges) {
            id2Edges_.put(edge.getEdgeId_(), edge);
        }
        // init nodes
        for (Node node : nodes) {
            id2Nodes_.put(node.getNodeId_(), node);
            // double check
            if (doubleCheck_) {
                for (int edgeId : node.getEdges_()) {
                    if (node.getNodeId_() != id2Edges_.get(edgeId).getSrcNodeId_()
                            && node.getNodeId_() != id2Edges_.get(edgeId).getDestNodeId_()) {
                        LOG.error("Wrong Graph data which occurs between node:{} and edge:{}.",
                                node.getNodeId_(), edgeId);
                    }
                }
            }

        }
    }

    public Node getNodeById(Integer id) {
        return id2Nodes_.get(id);
    }

    public Edge getEdgeById(Integer id) {
        return id2Edges_.get(id);
    }

    private static final Logger LOG = LoggerFactory.getLogger(StormMetricRegistry.class);
    private boolean doubleCheck_ = false;
    private Map<Integer, Node> id2Nodes_;
    private Map<Integer, Edge> id2Edges_;
    private int graphId_;


}
