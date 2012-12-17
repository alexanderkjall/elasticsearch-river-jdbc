package org.elasticsearch.river.jdbc;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: alexkjal
 * Date: 11/30/12
 * Time: 1:48 PM
 */
public class RiverConfiguration {
    private String riverIndexName;
    private String indexName;
    private int bulkSize;
    private TimeValue poll;
    private String url;
    private String user;
    private String password;
    private String sql;
    private int scale;
    private int rounding;
    private int maxBulkRequests;
    private TimeValue bulkTimeout;
    private RiverName riverName;
    private char delimiter;
    private int version;
    private String versionDigest;

    public RiverConfiguration() {
        poll = TimeValue.timeValueMinutes(60);
        url = null;
        user = null;
        password = null;
        sql = null;
        rounding = BigDecimal.ROUND_UP;
        scale = 0;
        bulkSize = 0;
        indexName = null;
        riverIndexName = null;
    }

    public RiverConfiguration(RiverSettings rs) {
        this();
        getJDBCValues(rs);
    }

    public void getJDBCValues(RiverSettings rs) {
        Map<String, Object> jdbcSettings = (Map<String, Object>) rs.settings().get("jdbc");

        if(jdbcSettings == null) {
            indexName = "jdbc";
            return;
        }

        poll = XContentMapValues.nodeTimeValue(jdbcSettings.get("poll"), poll);
        url = XContentMapValues.nodeStringValue(jdbcSettings.get("url"), url);
        user = XContentMapValues.nodeStringValue(jdbcSettings.get("user"), user);
        password = XContentMapValues.nodeStringValue(jdbcSettings.get("password"), password);
        sql = XContentMapValues.nodeStringValue(jdbcSettings.get("sql"), sql);
        rounding = parseRounding(XContentMapValues.nodeStringValue(jdbcSettings.get("rounding"), Integer.toString(rounding)));
        scale = XContentMapValues.nodeIntegerValue(jdbcSettings.get("scale"), scale);
        bulkSize = XContentMapValues.nodeIntegerValue(jdbcSettings.get("bulk_size"), bulkSize);
        indexName = XContentMapValues.nodeStringValue(jdbcSettings.get("index_name"), indexName);
        riverIndexName = XContentMapValues.nodeStringValue(jdbcSettings.get("river_index_name"), riverIndexName);

        if(indexName == null)
            indexName = guessIndexNameFromSql(sql);
        if(indexName == null)
            indexName = "jdbc";
    }

    private String guessIndexNameFromSql(String sql) {

        Pattern pattern = Pattern.compile(".*from\\s+(\\w*).*");

        Matcher matcher = pattern.matcher(sql);

        if(matcher.matches()) {
            return matcher.group(1);
        }

        return null;
    }

    public void getIndexValues(RiverSettings rs) {
        Map<String, Object> indexSettings = (Map<String, Object>) rs.settings().get("index");

        if(indexSettings == null) {
            return;
        }

        indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), "jdbc");
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

    protected int parseRounding(String inputRounding) {
        if ("ceiling".equalsIgnoreCase(inputRounding))
            return BigDecimal.ROUND_CEILING;
        else if ("down".equalsIgnoreCase(inputRounding))
            return BigDecimal.ROUND_DOWN;
        else if ("floor".equalsIgnoreCase(inputRounding))
            return BigDecimal.ROUND_FLOOR;
        else if ("halfdown".equalsIgnoreCase(inputRounding))
            return BigDecimal.ROUND_HALF_DOWN;
        else if ("halfeven".equalsIgnoreCase(inputRounding))
            return BigDecimal.ROUND_HALF_EVEN;
        else if ("halfup".equalsIgnoreCase(inputRounding))
            return BigDecimal.ROUND_HALF_UP;
        else if ("unnecessary".equalsIgnoreCase(inputRounding))
            return BigDecimal.ROUND_UNNECESSARY;
        else if ("up".equalsIgnoreCase(inputRounding))
            return BigDecimal.ROUND_UP;

        return BigDecimal.ROUND_CEILING;
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

    public void loadSavedState(Client client) throws IOException {
        if(client == null || riverName == null)
            return;

        GetResponse get = client.prepareGet(riverIndexName, riverName.name(), "_custom").execute().actionGet();
        if (get != null && get.exists()) {
            Map<String, Object> jdbcState = (Map<String, Object>) get.sourceAsMap().get("jdbc");
            if (jdbcState != null) {
                version = (Integer) jdbcState.get("version");
                version = version + 1; // increase to next version
                versionDigest = (String) jdbcState.get("digest");
            } else {
                throw new IOException("can't retrieve previously persisted state from " + riverIndexName + "/" + riverName.name());
            }
        }

    }

    public String getIndexName() {
        return indexName;
    }

    public void setRiverIndexName(String riverIndexName) {
        this.riverIndexName = riverIndexName;
    }

    public void setRiverName(RiverName riverName) {
        this.riverName = riverName;
    }
}
