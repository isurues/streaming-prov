package edu.indiana.d2i.flink.utils;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class PengProvState extends ProvState {

    public void handleNewEdgeGroup(List<ProvEdge> newStreamEdges) {
        List<ProvEdge> edgesToDelete = new ArrayList<>();
        List<ProvEdge> edgesToAdd = new ArrayList<>();
        for (ProvEdge newEdge : newStreamEdges) {
            String destId = newEdge.getDestination();
            if (edgesBySource.containsKey(destId)) {
                // edges with current source as the destination
                List<ProvEdge> edgesFromDest = edgesBySource.get(destId);
                for (ProvEdge e : edgesFromDest)
                    edgesToAdd.add(new ProvEdge(newEdge.getSource(), e.getDestination()));
                edgesToDelete.addAll(edgesFromDest);
            } else {
                // add only edges connected to an input data item
                if (StringUtils.countMatches(destId, "_") == 1)
                    addEdge(newEdge);
            }
        }
//        if (!edgesToAdd.isEmpty())
//            handleNewEdgeGroup(edgesToAdd);

        for (ProvEdge e : edgesToAdd)
            addEdge(e);

        for (ProvEdge e : edgesToDelete)
            deleteEdge(e);
    }



}
