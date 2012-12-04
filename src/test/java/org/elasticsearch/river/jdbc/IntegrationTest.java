package org.elasticsearch.river.jdbc;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

public class IntegrationTest extends AbstractNodesTest {

    private Client client;

    @Before
    public void createNodes() throws Exception {
        startNode("node1");
        startNode("node2");
        client = getClient();
    }

    @After
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node1");
    }

    @Test
    public void shard3docs100size120Unbalanced() throws Exception {
        testScroll(3, 100, 120);
    }

    private void testScroll(int numberOfShards, long numberOfDocs, int size) throws Exception {
        testScroll(numberOfShards, numberOfDocs, size, false);
    }

    private void testScroll(int numberOfShards, long numberOfDocs, int size, boolean unbalanced) throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", numberOfShards)).execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        Set<String> ids = new HashSet<String>();
        Set<String> expectedIds = new HashSet<String>();
        for (int i = 0; i < numberOfDocs; i++) {
            String id = Integer.toString(i);
            expectedIds.add(id);
            String routing = null;
            if (unbalanced) {
                if (i < (numberOfDocs * 0.6)) {
                    routing = "0";
                } else if (i < (numberOfDocs * 0.9)) {
                    routing = "1";
                } else {
                    routing = "2";
                }
            }
            client.prepareIndex("test", "type1", id).setRouting(routing).setSource("field", i).execute().actionGet();
            // make some segments
            if (i % 10 == 0) {
                client.admin().indices().prepareFlush().execute().actionGet();
            }
        }

        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch()
                .setSearchType(SearchType.SCAN)
                .setQuery(matchAllQuery())
                .setSize(size)
                .setScroll(TimeValue.timeValueMinutes(2))
                .execute().actionGet();

    }
}
