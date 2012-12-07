package org.elasticsearch.river.jdbc;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

/**
 * Created with IntelliJ IDEA.
 * User: capitol
 * Date: 2012-11-25
 * Time: 07:49
 */
public class BulkActionListener implements ActionListener<BulkResponse> {
    protected final ESLogger logger = Loggers.getLogger(getClass());

    private int numberOfDocsInRequest;

    public BulkActionListener(int numberOfDocsInRequest) {
        this.numberOfDocsInRequest = numberOfDocsInRequest;
    }

    @Override
    public void onResponse(BulkResponse bulkResponse) {

        if (bulkResponse.hasFailures()) {
            logger.error("bulk request has failures: {}", bulkResponse.buildFailureMessage());
        } else {
            logger.info("bulk request success ({} millis, {} docs, {} docs sent)", bulkResponse.tookInMillis(), bulkResponse.items().length, numberOfDocsInRequest );
        }
    }

    @Override
    public void onFailure(Throwable e) {
        logger.error("bulk request failed", e);
    }
}
