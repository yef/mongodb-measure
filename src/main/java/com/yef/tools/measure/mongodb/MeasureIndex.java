package com.yef.tools.measure.mongodb;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.conversions.Bson;

public class MeasureIndex extends BaseMeasure {

    private static final String COLLECTION_NAME = "testIndex";

    public static void main(String[] args) {
        MeasureIndex main = new MeasureIndex();
        main.measureNonIndexedOneByOneInsertion();
        main.measureIndexedOneByOneInsertion();
    }

    public void measureNonIndexedOneByOneInsertion() {
        int num = 3000;
        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);
        collection.drop();
        long timeBefore = System.currentTimeMillis();
        for (int i = 0; i < num; i++) {
            collection.insertOne(new Document("first", "value" + i).append("second", i + "second"));
        }
        Document statsAfter = database.runCommand(new Document("collStats", COLLECTION_NAME).append("scale", 1));
        long timeAfter = System.currentTimeMillis();
        printResults(num, statsAfter, timeBefore, timeAfter);
    }

    public void measureIndexedOneByOneInsertion() {
        int num = 30000;
        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);
        collection.drop();
        collection.createIndex(new Document("first", 1).append("second", 1));
        long timeBefore = System.currentTimeMillis();
        for (int i = 0; i < num; i++) {
            collection.insertOne(new Document("first", "value" + i).append("second", i + "second"));
        }
        Document statsAfter = database.runCommand(new Document("collStats", COLLECTION_NAME).append("scale", 1));
        long timeAfter = System.currentTimeMillis();
        printResults(num, statsAfter, timeBefore, timeAfter);
    }

}
