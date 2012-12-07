package org.elasticsearch.river.jdbc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: alexkjal
 * Date: 12/7/12
 * Time: 6:02 PM
 */
public class RowListenerCollector implements RowListener {
    private List<Map<String, Object>> results;

    public RowListenerCollector() {
        results = new ArrayList<Map<String, Object>>();
    }
    @Override
    public void row(IndexOperation operation, String type, String id, Map<String, Object> row) throws IOException {
        results.add(row);
    }

    @Override
    public void flush() {
    }

    public List<Map<String, Object>> getResults() {
        return results;
    }
}
