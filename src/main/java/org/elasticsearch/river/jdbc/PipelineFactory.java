package org.elasticsearch.river.jdbc;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.river.jdbc.PipeParts.ESEndPoint;
import org.elasticsearch.river.jdbc.PipeParts.RowTransformer;
import org.elasticsearch.river.jdbc.PipeParts.StatusUpdateRowListener;

/**
 * Created with IntelliJ IDEA.
 * User: capitol
 * Date: 2012-11-26
 * Time: 23:29
 */
public class PipelineFactory {
    public static RowListener createIncrementalPipe(String riverTableName, String indexTableName, Client client, int bulkSize, char delimiter) {
        ESEndPoint riverTableEnd = new ESEndPoint(riverTableName, client, 1);

        ESEndPoint end = new ESEndPoint(indexTableName, client, bulkSize);
        RowTransformer middle = new RowTransformer(end, delimiter);
        StatusUpdateRowListener start = new StatusUpdateRowListener(middle, riverTableEnd);

        return start;
    }
}
