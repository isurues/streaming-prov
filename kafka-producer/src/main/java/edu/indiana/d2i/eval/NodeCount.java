package edu.indiana.d2i.eval;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class NodeCount {

    public static void main(String[] args) {
        try {
            File file = new File("/Users/isuru/software/flink-1.3.1/output/foo");
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            String nodeId1;
            String nodeId2;
            Set<String> nodes = new HashSet<>();
            int edgeCount = 0;
            int byteCount = 0;
            while ((line = bufferedReader.readLine()) != null) {
                byteCount += line.getBytes().length;
                nodeId1 = line.substring(1, line.indexOf(',')).trim();
                nodeId2 = line.substring(line.indexOf(',') + 1, line.length() - 1).trim();
                nodes.add(nodeId1);
                nodes.add(nodeId2);
                edgeCount++;
            }
            System.out.println("Number of nodes = " + nodes.size());
            System.out.println("Number of edges = " + edgeCount);
            System.out.println("Size = " + byteCount);
            fileReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    public static void main(String[] args) {
//        String line = "<node1, node2>";
//        String nodeId1 = line.substring(1, line.indexOf(',')).trim();
//        String nodeId2 = line.substring(line.indexOf(',') + 1, line.length() - 1).trim();
//        System.out.println(nodeId1);
//        System.out.println(nodeId2);
//        Set<String> nodes = new HashSet<>();
//        nodes.add(nodeId1);
//        nodes.add(nodeId2);
//        nodes.add("aaaa");
//        nodes.add("aaaa");
//        nodes.add("aaaa");
//        System.out.println("Number of nodes = " + nodes.size());
//    }

}
