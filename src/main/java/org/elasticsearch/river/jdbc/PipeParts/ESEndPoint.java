package org.elasticsearch.river.jdbc.PipeParts;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.river.jdbc.BulkActionListener;
import org.elasticsearch.river.jdbc.IndexOperation;
import org.elasticsearch.river.jdbc.RowListener;

import java.io.IOException;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: capitol
 * Date: 2012-11-26
 * Time: 21:58
 */
public class ESEndPoint implements RowListener {
    private String indexTableName;
    private BulkRequestBuilder currentBulk;
    private int bulkSize;

    public ESEndPoint(String indexTableName, Client client, int bulkSize) {
        this.indexTableName = indexTableName;
        this.bulkSize = bulkSize;
        currentBulk = client.prepareBulk();
    }

    @Override
    public void row(IndexOperation operation, String type, String id, Map<String, Object> row) throws IOException {
        switch (operation) {
        case CREATE: {
            IndexRequest request = Requests.indexRequest(indexTableName).type(type).id(id).create(true).source(row);
            currentBulk.add(request);
            break;
        }
        case INDEX: {
            IndexRequest request = Requests.indexRequest(indexTableName).type(type).id(id).source(row);
            currentBulk.add(request);
            break;
        }
        case DELETE: {
            DeleteRequest request = Requests.deleteRequest(indexTableName).type(type).id(id);
            currentBulk.add(request);
            break;
        }
        }

        if (currentBulk.numberOfActions() >= bulkSize) {
            processBulk();
        }

    }

    @Override
    public void flush() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    private void processBulk() {
        while (onGoingBulks.intValue() >= maxActiveRequests) {
            logger.info("waiting for {} active bulk requests", onGoingBulks);
            synchronized (onGoingBulks) {
                try {
                    onGoingBulks.wait(millisBeforeContinue);
                } catch (InterruptedException e) {
                    logger.warn("timeout while waiting, continuing after {} ms", millisBeforeContinue);
                    totalTimeouts++;
                }
            }
        }
        int currentOnGoingBulks = onGoingBulks.incrementAndGet();
        final int numberOfActions = currentBulk.get().numberOfActions();
        logger.info("submitting new bulk request ({} docs, {} requests currently active)", numberOfActions, currentOnGoingBulks );
        try {
            currentBulk.get().execute(new BulkActionListener(logger, ack, numberOfActions, onGoingBulks, counter, riverName));
        } catch (Exception e) {
            logger.error("unhandled exception, failed to execute bulk request", e);
        } finally {
            currentBulk.set(client.prepareBulk());
        }
    }
}
