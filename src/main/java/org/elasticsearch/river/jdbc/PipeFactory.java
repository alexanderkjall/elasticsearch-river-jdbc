package org.elasticsearch.river.jdbc;

import org.elasticsearch.river.jdbc.PipeParts.ESEndPoint;
import org.elasticsearch.river.jdbc.PipeParts.RowTransformer;
import org.elasticsearch.river.jdbc.PipeParts.StatusUpdateRowListener;

/**
 * Created with IntelliJ IDEA.
 * User: capitol
 * Date: 2012-11-27
 * Time: 22:23
 */
public class PipeFactory {
    public static RowListener getJournalStrategy() {
        ESEndPoint end = new ESEndPoint(indexTableName, client, bulkSize, logger);
        RowTransformer middle = new RowTransformer(end, delimiter);
        StatusUpdateRowListener start = new StatusUpdateRowListener(middle, creationDate, client, riverIndexName, riverName, versionDigest, startTime);

        return start;
    }
}
