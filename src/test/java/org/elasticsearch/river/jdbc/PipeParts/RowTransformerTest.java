package org.elasticsearch.river.jdbc.PipeParts;

import org.elasticsearch.river.jdbc.IndexOperation;
import org.elasticsearch.river.jdbc.RowListenerCollector;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.assertEquals;

/**
 * Created with IntelliJ IDEA.
 * User: capitol
 * Date: 2012-12-09
 * Time: 08:11
 */
public class RowTransformerTest {
    @Test
    public void testRow() throws Exception {
        RowListenerCollector collector = new RowListenerCollector();

        RowTransformer instance = new RowTransformer(collector, '.');

        Map<String, Object> input = new HashMap<>();
        input.put("level1.level2", "value");

        instance.row(IndexOperation.INDEX, "", input);

        Map<String, Object> part = (Map<String, Object>)collector.getResults().get(0).get("level1");
        String result = (String)part.get("level2");

        assertEquals("check that the levels were split", "value", result);
    }

    @Test
    public void testRowTwoLevel2() throws Exception {
        RowListenerCollector collector = new RowListenerCollector();

        RowTransformer instance = new RowTransformer(collector, '.');

        Map<String, Object> input = new HashMap<>();
        input.put("level1.level2", "value");
        input.put("level1.level2-2", "value2");

        instance.row(IndexOperation.INDEX, "", input);

        Map<String, Object> part = (Map<String, Object>)collector.getResults().get(0).get("level1");
        String result1 = (String)part.get("level2");
        String result2 = (String)part.get("level2-2");

        assertEquals("check that the levels were split", "value", result1);
        assertEquals("and that the two parts are in the same map", "value2", result2);
    }
}
