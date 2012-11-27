package org.elasticsearch.river.jdbc.PipeParts;

import org.elasticsearch.client.Requests;
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
    private String indexTableName; // the index to write to

    public ESEndPoint(String indexTableName) {
        this.indexTableName = indexTableName;
    }

    @Override
    public void row(IndexOperation operation, String type, String id, Map<String, Object> row) throws IOException {
        switch (operation) {
        case CREATE:
            Requests.indexRequest(indexTableName).type(type).id(id).create(true).source(row);
            break;
        case INDEX:
            Requests.indexRequest(indexTableName).type(type).id(id).source(row);
            break;
        case DELETE:
            Requests.deleteRequest(indexTableName).type(type).id(id);
            break;
        }
    }

    @Override
    public void flush() {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
