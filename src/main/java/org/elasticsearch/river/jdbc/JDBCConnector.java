package org.elasticsearch.river.jdbc;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.river.jdbc.db.RiverDatabase;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created with IntelliJ IDEA.
 * User: capitol
 * Date: 2012-11-25
 * Time: 09:30
 */
public class JDBCConnector implements Runnable {
    protected final ESLogger logger = Loggers.getLogger(getClass());

    private RiverDatabase rdb;
    private RiverConfiguration rc;
    private AtomicBoolean closed;
    private Client client;
    private JDBCRiver.IndexState initialState;

    public JDBCConnector(RiverDatabase rdb, RiverConfiguration rc, AtomicBoolean closed, Client client, JDBCRiver.IndexState initialState) {
        this.rdb = rdb;
        this.rc = rc;
        this.closed = closed;
        this.client = client;
        this.initialState = initialState;
    }

    private void loadVersionAndDigest() throws IOException {
        client.admin().indices().prepareRefresh(rc.getRiverIndexName()).execute().actionGet();

        rc.loadSavedState(client);
    }

    private void readAllRows(RowListener pipe) {
        int rows = rdb.pushRowsToListener(rc.getFullIndexSql(), pipe);

        logger.info("got " + rows + " rows for full update at " + new Date());
    }

    private void readNewAndUpdatedRows(RowListener pipe, String startTime) throws IOException {
        int rows = rdb.pushRowsToListener(rc.getIndexSql(), pipe);

        logger.info("got " + rows + " rows for update at " + startTime);
    }

    private void readDeletes(RowListener pipe, String startTime) throws IOException {
        int rows = rdb.pushDeletesToListener(rc.getDeleteSql(), pipe);

        logger.info("got " + rows + " for deletion at " + startTime);
    }

    @Override
    public void run() {
        RowListener pipe = PipelineFactory.createIncrementalPipe(rc.getRiverName().toString(), rc.getIndexName(), client, rc.getBulkSize(), rc.getDelimiter(), rc.getType());

        if(initialState == JDBCRiver.IndexState.NEW) {
            readAllRows(pipe);
            pipe.refresh();
        }

        while (true) {
            try {
                loadVersionAndDigest();

                String startTime = rdb.getTime();

                readNewAndUpdatedRows(pipe, startTime);
                pipe.refresh();
                readDeletes(pipe, startTime);
                pipe.refresh();

                delay("next run");

            } catch (Exception e) {
                logger.error(e.getMessage(), e, (Object) null);
                closed.set(true);
            }
            if (closed.get()) {
                return;
            }
        }
    }

    private void delay(String reason) {
        if (rc.getPoll().millis() > 0L) {
            logger.info("{}, waiting {}, sql [{}] river table [{}]", reason, rc.getPoll(), rc.getIndexSql(), false);
            try {
                Thread.sleep(rc.getPoll().millis());
            } catch (InterruptedException ignored) {
            }
        }
    }
}
