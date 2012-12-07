package org.elasticsearch.river.jdbc;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
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
    private Date creationDate;

    public JDBCConnector(RiverDatabase rdb, RiverConfiguration rc, AtomicBoolean closed, Client client, Date creationDate) {
        this.rdb = rdb;
        this.rc = rc;
        this.closed = closed;
        this.client = client;
        this.creationDate = creationDate;
    }

    private VersionDigest loadVersionAndDigest() throws IOException {
        VersionDigest versionDigest = new VersionDigest(1L, null);

        client.admin().indices().prepareRefresh(rc.getRiverIndexName()).execute().actionGet();
        GetResponse get = client.prepareGet(rc.getRiverIndexName(), rc.getRiverName().name(), "_custom").execute().actionGet();
        if (creationDate != null || !get.exists()) {
        } else {
            Map<String, Object> jdbcState = (Map<String, Object>) get.sourceAsMap().get("jdbc");
            if (jdbcState != null) {
                versionDigest.setVersion((Number) jdbcState.get("version"));
                versionDigest.setVersion(versionDigest.getVersion() + 1); // increase to next version
                versionDigest.setDigest((String) jdbcState.get("digest"));
            } else {
                throw new IOException("can't retrieve previously persisted state from " + rc.getRiverIndexName() + "/" + rc.getRiverName().name());
            }
        }
        return versionDigest;
    }

    private int readNewAndUpdatedRows(RowListener pipe, VersionDigest versionDigest, String startTime) throws IOException {
        int rows = rdb.pushRowsToListener(rc.getSql(), pipe);

        logger.info("got " + rows + " rows for version " + versionDigest.getVersion() + ", digest = " + versionDigest.getDigest());

        return rows;
    }

    private int readDeletes(RowListener pipe) throws IOException {
        int rows = rdb.pushDeletesToListener(rc.getSql(), pipe);

        logger.info("got " + rows + " for deletion");

        return rows;
    }

    @Override
    public void run() {

        while (true) {
            try {
                VersionDigest versionDigest = loadVersionAndDigest();

                String startTime = rdb.getTime();
                RowListener pipe = PipelineFactory.createIncrementalPipe(rc.getRiverName().toString(), rc.getRiverIndexName(), client, rc.getBulkSize(), rc.getDelimiter());

                int nrOfUpdates = readNewAndUpdatedRows(pipe, versionDigest, startTime);
                pipe.flush();
                int nrOfDeletes = readDeletes(pipe);
                pipe.flush();

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
            logger.info("{}, waiting {}, sql [{}] river table [{}]",
                    reason, rc.getPoll(), rc.getSql(), false);
            try {
                Thread.sleep(rc.getPoll().millis());
            } catch (InterruptedException ignored) {
            }
        }
    }
}
