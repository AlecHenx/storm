package org.apache.storm.utils;

import java.util.List;
import java.util.Map;
import org.apache.storm.metrics2.StormMetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Graph {

    public Graph(int graphId, List<Node> nodes, List<Edge> edges, boolean doubleCheck) {
        this.graphId = graphId;
        internalLoadGraph(nodes, edges);
        this.doubleCheck = doubleCheck;
    }

    public Graph(int graphId, List<Node> nodes, List<Edge> edges) {
        new Graph(graphId, nodes, edges, false);
    }

    public void internalLoadGraph(List<Node> nodes, List<Edge> edges) {
        // init edges
        for (Edge edge : edges) {
            id2Edges.put(edge.getEdgeId(), edge);
        }
        // init nodes
        for (Node node : nodes) {
            id2Nodes.put(node.getNodeId(), node);
            // double check
            if (doubleCheck) {
                for (int edgeId : node.getEdges()) {
                    if (node.getNodeId() != id2Edges.get(edgeId).getSrcNodeId()
                        && node.getNodeId() != id2Edges.get(edgeId).getDestNodeId()) {
                        LOG.error("Wrong Graph data which occurs between node:{} and edge:{}.",
                            node.getNodeId(), edgeId);
                    }
                }
            }

        }
    }

    public Node getNodeById(Integer id) {
        return id2Nodes.get(id);
    }

    public Edge getEdgeById(Integer id) {
        return id2Edges.get(id);
    }

    private static final Logger LOG = LoggerFactory.getLogger(StormMetricRegistry.class);
    private boolean doubleCheck = false;
    private Map<Integer, Node> id2Nodes;
    private Map<Integer, Edge> id2Edges;
    private int graphId;


}
