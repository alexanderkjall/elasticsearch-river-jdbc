package org.elasticsearch.river.jdbc.PipeParts;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.river.jdbc.BulkActionListener;
import org.elasticsearch.river.jdbc.IndexOperation;
import org.elasticsearch.river.jdbc.RefreshActionListener;
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
    protected final ESLogger logger = Loggers.getLogger(getClass());

    private String indexTableName;
    private BulkRequestBuilder currentBulk;
    private Client client;
    private int bulkSize;
    private String type;

    public ESEndPoint(String indexTableName, Client client, int bulkSize, String type) {
        this.indexTableName = indexTableName;
        this.client = client;
        this.bulkSize = bulkSize;
        this.type = type;
        currentBulk = client.prepareBulk();
    }

    @Override
    public void row(IndexOperation operation, String id, Map<String, Object> row) throws IOException {
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
        case DELETE:
            DeleteRequest request = Requests.deleteRequest(indexTableName).type(type).id(id);
            currentBulk.add(request);
            break;
        }

        if (currentBulk.numberOfActions() >= bulkSize) {
            processBulk();
        }

    }

    @Override
    public void refresh() {
        processBulk();

        client.admin().indices().prepareRefresh(indexTableName).execute(new RefreshActionListener());
    }

    private void processBulk() {
        int numberOfActions = currentBulk.numberOfActions();
        logger.info("submitting new bulk request ({} docs)", numberOfActions );
        try {
            currentBulk.execute(new BulkActionListener(numberOfActions));
        } catch (Exception e) {
            logger.error("unhandled exception, failed to execute bulk request", e);
        } finally {
            currentBulk = client.prepareBulk();
        }
    }
}
