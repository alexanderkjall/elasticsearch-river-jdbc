package org.elasticsearch.river.jdbc.PipeParts;

import org.elasticsearch.client.Client;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.jdbc.ElasticSearchUtil;
import org.elasticsearch.river.jdbc.IndexOperation;
import org.elasticsearch.river.jdbc.RowListener;
import org.elasticsearch.river.jdbc.VersionDigest;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: capitol
 * Date: 2012-11-25
 * Time: 22:36
 */
public class StatusUpdateRowListener implements RowListener {
    private RowListener next;
    private final Date creationDate;
    private final Client client;
    private final String riverIndexName;
    private final RiverName riverName;
    private final VersionDigest versionDigest;
    private final String startTime;
    private int rows;

    public StatusUpdateRowListener(RowListener next, Date creationDate, Client client, String riverIndexName, RiverName riverName, VersionDigest versionDigest, String startTime) {

        this.next = next;
        this.creationDate = creationDate;
        this.client = client;
        this.riverIndexName = riverIndexName;
        this.riverName = riverName;
        this.versionDigest = versionDigest;
        this.startTime = startTime;
    }

    @Override
    public void row(IndexOperation operation, String type, String id, Map<String, Object> row) throws IOException {
        rows++;
        if (rows % 1000 == 0)
            ElasticSearchUtil.saveStatus(creationDate, client, riverIndexName, riverName, versionDigest.getVersion(), versionDigest.getDigest(), "running", rows, startTime);

        next.row(operation, type, id, row);
    }

    @Override
    public void flush() {
        rows = 0;
        next.flush();
    }
}
