package com.yef.tools.measure.mongodb;

import com.mongodb.client.MongoCollection;
import org.bson.Document;

public class MultithreadDocumentInsertion {

    private static final String COLLECTION_NAME = "testCollectionMultiThread";

    public static void main(String[] args) {
        MultithreadDocumentInsertion main = new MultithreadDocumentInsertion();
        int num = 2500;
        int countThreads = 400;
        for (int i = 0; i < countThreads; i++) {
            Thread thread = new Thread(new InserterThread(num, countThreads));
            thread.start();
        }
    }


    public static class InserterThread extends BaseMeasure implements Runnable {

        private int num;
        private int countThreads;

        public InserterThread(int num, int countThreads) {
            this.num = num;
            this.countThreads = countThreads;
        }

        @Override
        public void run() {
            String threadName = Thread.currentThread().getName();
            System.out.println("Runned thread #" + threadName);
            MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);
            long timeBefore = System.currentTimeMillis();
            long maxInsertionTime = 0L;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < num; i++) {
                long timeBeforeInsertion = System.currentTimeMillis();
                collection.insertOne(new Document("key1", "value" + threadName + "-" + i));
                long timeAfterInsertion = System.currentTimeMillis();
                long insertionTime = timeAfterInsertion - timeBeforeInsertion;
                if (i < 10) { // output only first 10 documents
                    sb.append("Insertion time of ").append(i).append(" element:").append(insertionTime).append("\n");
                }
                if (insertionTime > maxInsertionTime) {
                    maxInsertionTime = insertionTime;
                }
            }
            Document statsAfter = database.runCommand(new Document("collStats", COLLECTION_NAME).append("scale", 1));
            long timeAfter = System.currentTimeMillis();
            System.out.println("Statistics for thread #" + threadName + ":");
            synchronized (this) {
                printResults(num, statsAfter, timeBefore, timeAfter, "Max insertion time: " + maxInsertionTime + " ms", sb.toString());
                long totalTime = ((timeAfter - timeBefore) / 1000);
                int totalDocuments = num * countThreads;
                long rate = totalDocuments / totalTime;
                System.out.println("\n\n" + totalDocuments + " documents were inserted in " + totalTime
                        + " sec (" + rate + " requests per second, " + (rate * 60 * 60 * 24) + " request per day)\n\n----------------------------");
            }
        }
    }

}
