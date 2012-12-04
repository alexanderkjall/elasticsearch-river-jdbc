package org.elasticsearch.river.jdbc;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: alexkjal
 * Date: 11/30/12
 * Time: 1:48 PM
 */
public class RiverConfiguration {
    private String riverIndexName;
    private String indexName;
    private String typeName;
    private int bulkSize;
    private TimeValue poll;
    private String url;
    private String user;
    private String password;
    private String sql;
    private int scale;
    private String rounding;
    private int maxBulkRequests;
    private TimeValue bulkTimeout;
    private RiverName riverName;
    private char delimiter;

    public RiverConfiguration() {

    }

    public RiverConfiguration(RiverSettings rs) {
        getJDBCValues(rs);
    }

    public void getJDBCValues(RiverSettings rs) {
        Map<String, Object> jdbcSettings = (Map<String, Object>) rs.settings().get("jdbc");
        poll = XContentMapValues.nodeTimeValue(jdbcSettings.get("poll"), TimeValue.timeValueMinutes(60));
        url = XContentMapValues.nodeStringValue(jdbcSettings.get("url"), null);
        user = XContentMapValues.nodeStringValue(jdbcSettings.get("user"), null);
        password = XContentMapValues.nodeStringValue(jdbcSettings.get("password"), null);
        sql = XContentMapValues.nodeStringValue(jdbcSettings.get("sql"), null);
        rounding = XContentMapValues.nodeStringValue(jdbcSettings.get("rounding"), null);
        scale = XContentMapValues.nodeIntegerValue(jdbcSettings.get("scale"), 0);
        bulkSize = XContentMapValues.nodeIntegerValue(jdbcSettings.get("bulk_size"), 0);
        typeName = XContentMapValues.nodeStringValue(jdbcSettings.get("type_name"), null);
        indexName = XContentMapValues.nodeStringValue(jdbcSettings.get("index_name"), null);
        riverIndexName = XContentMapValues.nodeStringValue(jdbcSettings.get("river_index_name"), null);
    }

    public void getIndexValues(RiverSettings rs) {
        Map<String, Object> indexSettings = (Map<String, Object>) rs.settings().get("index");
        indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), "jdbc");
        typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), "jdbc");
        bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
        maxBulkRequests = XContentMapValues.nodeIntegerValue(indexSettings.get("max_bulk_requests"), 30);
        if (indexSettings.containsKey("bulk_timeout")) {
            bulkTimeout = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(indexSettings.get("bulk_timeout"), "60s"), TimeValue.timeValueMillis(60000));
        } else {
            bulkTimeout = TimeValue.timeValueMillis(60000);
        }

        /** defaults
         indexName = "jdbc";
         typeName = "jdbc";
         bulkSize = 100;
         maxBulkRequests = 30;
         bulkTimeout = TimeValue.timeValueMillis(60000);

         */
    }

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getRiverIndexName() {
        return riverIndexName;
    }

    public String getSql() {
        return sql;
    }

    public String getTypeName() {
        return typeName;
    }

    public TimeValue getPoll() {
        return poll;
    }

    public RiverName getRiverName() {
        return riverName;
    }

    public int getBulkSize() {
        return bulkSize;
    }

    public char getDelimiter() {
        return delimiter;
    }
}
