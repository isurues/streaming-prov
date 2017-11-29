package edu.indiana.d2i.eval;

import org.apache.htrace.fasterxml.jackson.databind.JsonNode;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.htrace.fasterxml.jackson.databind.node.ArrayNode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class InputNodeCount {

    public static void main(String[] args) {
        try {
            File file = new File("/Users/isuru/Desktop/ccgrid-2018/experiments/mr-prov");
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            ObjectMapper objectMapper = new ObjectMapper();
            String line;
            Set<String> nodes = new HashSet<>();
            int edgeCount = 0, lineCount = 0, groupCount = 0, edgesInGropus = 0;
            while ((line = bufferedReader.readLine()) != null) {
                lineCount++;
                JsonNode jsonNode = objectMapper.readTree(line);
                if (jsonNode.get("group") != null) {
                    groupCount++;
                    ArrayNode array = (ArrayNode) jsonNode.get("group");
                    Iterator<JsonNode> it = array.elements();
                    while (it.hasNext()) {
                        edgesInGropus++;
                        JsonNode node = it.next();
                        nodes.add(node.get("sourceId").asText());
                        nodes.add(node.get("destId").asText());
                        edgeCount++;
                    }
                } else {
                    nodes.add(jsonNode.get("sourceId").asText());
                    nodes.add(jsonNode.get("destId").asText());
                    edgeCount++;
                }
            }
            System.out.println("Number of nodes = " + nodes.size());
            System.out.println("Number of edges = " + edgeCount);
            System.out.println("Number of lines = " + lineCount);
            System.out.println("Number of groups = " + groupCount);
            System.out.println("Number of edges in groups = " + edgesInGropus);
            fileReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
