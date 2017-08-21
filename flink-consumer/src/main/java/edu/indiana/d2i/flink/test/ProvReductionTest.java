package edu.indiana.d2i.flink.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.indiana.d2i.flink.utils.ProvState;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class ProvReductionTest {

    public static void main(String[] args) {
        File file = new File("/Users/isuru/research/streaming-prov/flink-consumer/prov.txt");
        ObjectMapper mapper = new ObjectMapper();
        ProvState processor = new ProvState();
        try (Scanner scanner = new Scanner(file)) {
            while (scanner.hasNextLine()) {
                processor.processNotification((ObjectNode) mapper.readTree(scanner.nextLine()));
            }
            scanner.close();
            processor.printState();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



}
