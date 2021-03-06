package com.yef.tools.measure.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class BaseMeasure {

    private static final String MONGODB_HOST = "localhost";
    private static final Integer MONGODB_PORT = 27017;
    private static final String DATABASE_NAME = "testDb";

    protected MongoClient mongoClient;
    protected MongoDatabase database;

    public BaseMeasure() {
        mongoClient = new MongoClient(MONGODB_HOST, MONGODB_PORT);
        database = mongoClient.getDatabase(DATABASE_NAME);
    }

    public void printResults(int num, Document statsAfter, long timeBefore, long timeAfter, String... addition) {
        System.out.println("-----------------------");
        System.out.println("Count of documents in collection: " + num);
        System.out.println("Collection Size: " + statsAfter.get("size") + " bytes");
        System.out.println("Size per document from stats: " + statsAfter.get("avgObjSize") + " bytes");
        System.out.println("Calculated size per document: " + ((double) statsAfter.getInteger("size")) / num + " bytes");
        System.out.println("Total time for insertion " + num + " documents: " + ((double) (timeAfter - timeBefore)) / 1000 + " s");
        System.out.println("Insertion time per document: " + ((double) (timeAfter - timeBefore)) / num + " ms");
        for (String string : addition) System.out.println(string);
        System.out.println("-----------------------");
    }
}
