package org.elasticsearch.river.jdbc.db;

import org.junit.Test;

import javax.sql.rowset.serial.SerialArray;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created with IntelliJ IDEA.
 * User: capitol
 * Date: 2012-12-21
 * Time: 20:31
 */
public class SQLUtilTest {
    private static Map<Integer, Class> mapping = new HashMap<>();

    static {
        mapping.put(Types.CHAR, String.class);
        mapping.put(Types.VARCHAR, String.class);
        mapping.put(Types.LONGVARCHAR, String.class);
        mapping.put(Types.NCHAR, String.class);
        mapping.put(Types.NVARCHAR, String.class);
        mapping.put(Types.LONGNVARCHAR, String.class);
        mapping.put(Types.BINARY, byte[].class);
        mapping.put(Types.VARBINARY, byte[].class);
        mapping.put(Types.LONGVARBINARY, byte[].class);
        mapping.put(Types.ARRAY, String.class);
        mapping.put(Types.BIGINT, Long.class);
        mapping.put(Types.BIT, Integer.class);
        mapping.put(Types.BOOLEAN, Boolean.class);
        mapping.put(Types.BLOB, byte[].class);
        mapping.put(Types.CLOB, String.class);
        mapping.put(Types.NCLOB, String.class);
        mapping.put(Types.DATALINK, URL.class);
        mapping.put(Types.DATE, String.class);
        mapping.put(Types.TIME, String.class);
        mapping.put(Types.TIMESTAMP, String.class);
        mapping.put(Types.DECIMAL, Double.class);
        mapping.put(Types.NUMERIC, Double.class);
        mapping.put(Types.DOUBLE, Double.class);
        mapping.put(Types.FLOAT, Double.class);
        mapping.put(Types.INTEGER, Integer.class);
        mapping.put(Types.OTHER, Object.class);
        mapping.put(Types.JAVA_OBJECT, Object.class);
        mapping.put(Types.REAL, Float.class);
        mapping.put(Types.SMALLINT, Integer.class);
        mapping.put(Types.SQLXML, String.class);
        mapping.put(Types.TINYINT, Integer.class);
        mapping.put(Types.NULL, null);
        mapping.put(Types.DISTINCT, null);
        mapping.put(Types.STRUCT, null);
        mapping.put(Types.REF, null);
        mapping.put(Types.ROWID, null);
    }

    private static ResultSet setupMockResultSet() throws SQLException, MalformedURLException {
        ResultSet rs = mock(ResultSet.class);

        when(rs.getString(anyInt())).thenReturn("");
        when(rs.getNString(anyInt())).thenReturn("");
        when(rs.getBytes(anyInt())).thenReturn(new byte[]{});

        Array a = mock(Array.class);
        when(a.getArray()).thenReturn(new Integer[10]);
        Array sa = new SerialArray(a);
        when(rs.getArray(anyInt())).thenReturn(sa);

        when(rs.getLong(anyInt())).thenReturn(10L);
        when(rs.getInt(anyInt())).thenReturn(10);
        when(rs.getBoolean(anyInt())).thenReturn(true);

        Blob blob = mock(Blob.class);
        when(blob.getBytes(anyLong(), anyInt())).thenReturn(new byte[]{});
        when(rs.getBlob(anyInt())).thenReturn(blob);
        Clob c = mock(Clob.class);
        when(c.getSubString(anyInt(), anyInt())).thenReturn("");
        when(rs.getClob(anyInt())).thenReturn(c);
        NClob nc = mock(NClob.class);
        when(nc.getSubString(anyInt(), anyInt())).thenReturn("");
        when(rs.getNClob(anyInt())).thenReturn(nc);
        SQLXML sx = mock(SQLXML.class);
        when(sx.getString()).thenReturn("");
        when(rs.getSQLXML(anyInt())).thenReturn(sx);

        when(rs.getURL(anyInt())).thenReturn(new URL("http", "localhost", 80, ""));
        when(rs.getDate(anyInt())).thenReturn(new Date(100L));
        when(rs.getTime(anyInt())).thenReturn(new Time(100L));
        when(rs.getTimestamp(anyInt())).thenReturn(new Timestamp(100L));
        when(rs.getBigDecimal(anyInt())).thenReturn(new BigDecimal("100"));
        when(rs.getDouble(anyInt())).thenReturn(100.0);
        when(rs.getFloat(anyInt())).thenReturn(100.0f);
        when(rs.getObject(anyInt())).thenReturn(new Object());

        return rs;
    }

    @Test
    public void testParseType() throws IOException, SQLException {
        ResultSet rs = setupMockResultSet();

        for(Map.Entry<Integer, Class> entry : mapping.entrySet()) {
            Object result = SQLUtil.parseType(entry.getKey(), rs, 1, 1, 1);

            if(entry.getValue() == null)
                assertNull("", result);
            else
                assertTrue("check that its correct type, wanted " + entry.getValue() + " but got " + result, entry.getValue().isInstance(result));
        }
    }
}
