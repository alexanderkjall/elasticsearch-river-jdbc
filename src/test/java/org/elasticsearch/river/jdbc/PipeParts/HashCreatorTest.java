package org.elasticsearch.river.jdbc.PipeParts;

import org.elasticsearch.river.jdbc.IndexOperation;
import org.elasticsearch.river.jdbc.RowListenerCollector;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created with IntelliJ IDEA.
 * User: alexkjal
 * Date: 12/10/12
 * Time: 11:53 AM
 */
public class HashCreatorTest {
    @Test
    public void testRow() throws Exception {
        RowListenerCollector collector = new RowListenerCollector();
        HashCreator instance = new HashCreator(collector);

        Map<String, Object> map = new HashMap<String, Object>();
        map.put("_id", "1");
        map.put("content", "some data");

        instance.row(IndexOperation.INDEX, "", "", map);

        assertNotNull("check that _content_hash was created", collector.getResults().get(0).get("_content_hash"));
        assertEquals("check content of hash", "e58a43818fced9da54dc7210fbd6869491878a3b56c36f41392866b25f7218", collector.getResults().get(0).get("_content_hash"));
    }
}
