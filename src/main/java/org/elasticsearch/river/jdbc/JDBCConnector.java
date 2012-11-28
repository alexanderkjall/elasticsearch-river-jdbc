package org.elasticsearch.river.jdbc;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.river.RiverName;

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
    private RiverDatabase rdb;
    private RiverName riverName;
    private ESLogger logger;
    private String sql;
    private AtomicBoolean closed;
    private Client client;
    private String riverIndexName;
    private Date creationDate;
    private TimeValue poll;
    private String indexName;
    private String typeName;
    private int bulkSize;

    public JDBCConnector(RiverDatabase rdb, RiverName riverName,
                         ESLogger logger, String sql, AtomicBoolean closed,
                         Client client, String riverIndexName, Date creationDate, TimeValue poll, String indexName,
                         String typeName, int bulkSize) {
        this.rdb = rdb;
        this.riverName = riverName;
        this.logger = logger;
        this.sql = sql;
        this.closed = closed;
        this.client = client;
        this.riverIndexName = riverIndexName;
        this.creationDate = creationDate;
        this.poll = poll;
        this.indexName = indexName;
        this.typeName = typeName;
        this.bulkSize = bulkSize;
    }

    private VersionDigest loadVersionAndDigest() throws IOException {
        VersionDigest versionDigest = new VersionDigest(1L, null);

        client.admin().indices().prepareRefresh(riverIndexName).execute().actionGet();
        GetResponse get = client.prepareGet(riverIndexName, riverName.name(), "_custom").execute().actionGet();
        if (creationDate != null || !get.exists()) {
        } else {
            Map<String, Object> jdbcState = (Map<String, Object>) get.sourceAsMap().get("jdbc");
            if (jdbcState != null) {
                versionDigest.setVersion((Number) jdbcState.get("version"));
                versionDigest.setVersion(versionDigest.getVersion() + 1); // increase to next version
                versionDigest.setDigest((String) jdbcState.get("digest"));
            } else {
                throw new IOException("can't retrieve previously persisted state from " + riverIndexName + "/" + riverName.name());
            }
        }
        return versionDigest;
    }

    private int readNewAndUpdatedRows(RowListener pipe, VersionDigest versionDigest, String startTime) throws IOException {
        ElasticSearchUtil.saveStatus(creationDate, client, riverIndexName, riverName, versionDigest.getVersion(), versionDigest.getDigest(), "running", 0, startTime );

        int rows = rdb.pushRowsToListener(sql, pipe);

        logger.info("got " + rows + " rows for version " + versionDigest.getVersion() + ", digest = " + versionDigest.getDigest());

        return rows;
    }

    private int readDeletes(RowListener pipe) throws IOException {
        int rows = rdb.pushDeletesToListener(sql, pipe);

        logger.info("got " + rows + " for deletion");

        return rows;
    }

    @Override
    public void run() {

        while (true) {
            try {
                VersionDigest versionDigest = loadVersionAndDigest();

                String startTime = rdb.getStartTime();
                RowListener pipe = PipelineFactory.createIncrementalPipe();

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
        if (poll.millis() > 0L) {
            logger.info("{}, waiting {}, sql [{}] river table [{}]",
                    reason, poll, sql, false);
            try {
                Thread.sleep(poll.millis());
            } catch (InterruptedException ignored) {
            }
        }
    }
}
