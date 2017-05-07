package com.yef.tools.measure.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.codecs.UuidCodec;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.ArrayList;
import java.util.List;

public class MeasureDocumentInsertion {

    private static final String MONGODB_HOST = "localhost";
    private static final Integer MONGODB_PORT = 27017;
    private static final String DATABASE_NAME = "testDb";
    public static final String COLLECTION_NAME = "testCollection";
    MongoClient mongoClient;
    MongoDatabase database;


    public MeasureDocumentInsertion() {
        mongoClient = new MongoClient(MONGODB_HOST, MONGODB_PORT);
        CodecRegistry codecRegistry =
                CodecRegistries.fromRegistries(CodecRegistries.fromCodecs(new UuidCodec(UuidRepresentation.STANDARD)),
                        MongoClient.getDefaultCodecRegistry());
        MongoClientOptions options = MongoClientOptions.builder()
                .codecRegistry(codecRegistry).build();
        MongoClient client = new MongoClient(new ServerAddress(), options);
        database = client.getDatabase(DATABASE_NAME);
    }

    public static void main(String[] args) {
        MeasureDocumentInsertion main = new MeasureDocumentInsertion();
        main.measureOneByOneInsertion();
        main.measureBulkInsertion();
    }

    private void measureOneByOneInsertion() {
        int num = 100000;
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
        int num = 1000000;
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

    private void printResults(int num, Document statsAfter, long timeBefore, long timeAfter, String... addition) {
        System.out.println("-----------------------");
        System.out.println("Count of documents in collection '" + COLLECTION_NAME + "': " + num);
        System.out.println("Collection Size: " + statsAfter.get("size") + " bytes");
        System.out.println("Size per document from stats: " + statsAfter.get("avgObjSize") + " bytes");
        System.out.println("Calculated size per document: " + ((double) statsAfter.getInteger("size")) / num + " bytes");
        System.out.println("Total time for insertion " + num + " documents: " + ((double) (timeAfter - timeBefore)) / 1000 + " s");
        System.out.println("Insertion time per document: " + ((double) (timeAfter - timeBefore)) / num + " ms");
        for (String string : addition) System.out.println(string);
        System.out.println("-----------------------");
    }
}
