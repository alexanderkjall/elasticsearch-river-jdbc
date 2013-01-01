package org.elasticsearch.river.jdbc.PipeParts;

import org.elasticsearch.river.jdbc.IndexOperation;
import org.elasticsearch.river.jdbc.RowListenerCollector;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.assertEquals;

/**
 * Created with IntelliJ IDEA.
 * User: alexkjal
 * Date: 12/10/12
 * Time: 12:42 PM
 */
public class StatusUpdateRowListenerTest {
    @Test
    public void testRow() throws Exception {
        RowListenerCollector prime = new RowListenerCollector();
        RowListenerCollector statuses = new RowListenerCollector();

        Map<String, Object> map = new HashMap<>();

        StatusUpdateRowListener instance = new StatusUpdateRowListener(prime, statuses);

        for(int i = 0; i < 2000; i++) {
            instance.row(IndexOperation.INDEX, "", map);
        }

        assertEquals("check that we get a status update for every thousand rows", 2, statuses.getResults().size());
    }

    @Test
    public void testFlush() throws Exception {

    }
}
