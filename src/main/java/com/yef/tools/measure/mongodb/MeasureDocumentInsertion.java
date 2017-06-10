package com.yef.tools.measure.mongodb;

import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class MeasureDocumentInsertion extends BaseMeasure {

    private static final String COLLECTION_NAME = "testCollection";

    public static void main(String[] args) {
        MeasureDocumentInsertion main = new MeasureDocumentInsertion();
        main.measureOneByOneInsertion();
        main.measureBulkInsertion();
    }

    private void measureOneByOneInsertion() {
        int num = 1000;
        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);
        collection.drop();
        long timeBefore = System.currentTimeMillis();
        for (int i = 0; i < num; i++) {
            collection.insertOne(new Document("key1", "value" + i));
        }
        Document statsAfter = database.runCommand(new Document("collStats", COLLECTION_NAME).append("scale", 1));
        long timeAfter = System.currentTimeMillis();
        printResults(num, statsAfter, timeBefore, timeAfter);
    }

    private void measureBulkInsertion() {
        int num = 10000;
        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);
        collection.drop();
        System.gc();
        Runtime rt = Runtime.getRuntime();
        long memoryBefore = rt.totalMemory() - rt.freeMemory();
        long timeBefore = System.currentTimeMillis();
        List<Document> docs = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            docs.add(new Document("key1", "value" + i));
        }
        collection.insertMany(docs);
        Document statsAfter = database.runCommand(new Document("collStats", COLLECTION_NAME).append("scale", 1));
        long timeAfter = System.currentTimeMillis();
        System.gc();
        long memoryAfter = rt.totalMemory() - rt.freeMemory();
        printResults(num, statsAfter, timeBefore, timeAfter, "JVM memory used for collection: " + (memoryAfter - memoryBefore) + " bytes");
    }

}
