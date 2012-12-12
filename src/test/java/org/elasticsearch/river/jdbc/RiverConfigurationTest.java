package org.elasticsearch.river.jdbc;

import org.elasticsearch.river.RiverSettings;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
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
        jdbc.put("sql", "select * from orders");
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
}
