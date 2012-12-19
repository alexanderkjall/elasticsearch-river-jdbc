package org.elasticsearch.river.jdbc;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created with IntelliJ IDEA.
 * User: alexkjal
 * Date: 12/11/12
 * Time: 2:05 PM
 */
public class RiverConfigurationTest {
    @Test
    public void testGetJDBCValues() throws Exception {
        RiverConfiguration instance = new RiverConfiguration();

        RiverSettings rs = mock(RiverSettings.class);

        instance.getJDBCValues(rs);

        assertEquals("check that we have good defaults", "jdbc", instance.getIndexName());
    }

    @Test
    public void testGetIndexNameWithSqlGuess() throws Exception {
        RiverConfiguration instance = new RiverConfiguration();

        RiverSettings rs = mock(RiverSettings.class);

        Map<String, Object> settings = new HashMap<String, Object>();
        Map<String, Object> jdbc = new HashMap<String, Object>();
        jdbc.put("indexSql", "select * from orders");
        settings.put("jdbc", jdbc);

        when(rs.settings()).thenReturn(settings);

        instance.getJDBCValues(rs);

        assertEquals("check that we have good defaults", "orders", instance.getIndexName());
    }

    @Test
    public void testGetIndexValuesVerifyNoNPE() throws Exception {
        RiverConfiguration instance = new RiverConfiguration();

        RiverSettings rs = mock(RiverSettings.class);

        instance.getIndexValues(rs);
    }

    @Test
    public void testLoadSavedStateCheckHandleNull() throws Exception {
        RiverConfiguration instance = new RiverConfiguration();

        instance.loadSavedState(null);
    }

    @Test
    public void testLoadSavedStateCheckHandleNull2() throws Exception {
        RiverConfiguration instance = new RiverConfiguration();

        Client client = mock(Client.class);

        instance.loadSavedState(client);
    }

    @Test
    public void testLoadSavedStateWithRiverName() throws Exception {
        RiverConfiguration instance = new RiverConfiguration();

        Client client = mock(Client.class);
        RiverName rn = mock(RiverName.class);
        ListenableActionFuture<GetResponse> f = mock(ListenableActionFuture.class);
        GetRequestBuilder grb = mock(GetRequestBuilder.class);
        when(grb.execute()).thenReturn(f);
        when(client.prepareGet(anyString(), anyString(), anyString())).thenReturn(grb);

        instance.setRiverName(rn);
        instance.loadSavedState(client);
    }
}
