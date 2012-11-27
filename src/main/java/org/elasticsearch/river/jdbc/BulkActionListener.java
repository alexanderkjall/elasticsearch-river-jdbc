package org.elasticsearch.river.jdbc;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.logging.ESLogger;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created with IntelliJ IDEA.
 * User: capitol
 * Date: 2012-11-25
 * Time: 07:49
 */
public class BulkActionListener implements ActionListener<BulkResponse> {
    private final ESLogger logger;
    private final BulkAcknowledge ack;
    private final int numberOfActions;
    private final AtomicInteger onGoingBulks;
    private final AtomicInteger counter;
    private final String riverName;

    public BulkActionListener(ESLogger logger, BulkAcknowledge ack, int numberOfActions, AtomicInteger onGoingBulks, AtomicInteger counter, String riverName) {
        this.logger = logger;
        this.ack = ack;
        this.numberOfActions = numberOfActions;
        this.onGoingBulks = onGoingBulks;
        this.counter = counter;
        this.riverName = riverName;
    }

    @Override
    public void onResponse(BulkResponse bulkResponse) {
        if (ack != null) try {
            ack.acknowledge(riverName, bulkResponse.items());
        } catch (IOException ex) {
            logger.error("bulk acknowledge failed", ex);
        }
        if (bulkResponse.hasFailures()) {
            logger.error("bulk request has failures: {}", bulkResponse.buildFailureMessage());
        } else {
            final int totalActions = counter.addAndGet(numberOfActions);
            logger.info("bulk request success ({} millis, {} docs, total of {} docs)", bulkResponse.tookInMillis(), numberOfActions, totalActions );
        }
        onGoingBulks.decrementAndGet();
        synchronized (onGoingBulks) {
            onGoingBulks.notifyAll();
        }
    }

    @Override
    public void onFailure(Throwable e) {
        logger.error("bulk request failed", e);
    }
}
