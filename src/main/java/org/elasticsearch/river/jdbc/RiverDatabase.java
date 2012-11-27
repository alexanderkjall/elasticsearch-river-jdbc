package org.elasticsearch.river.jdbc;

import org.elasticsearch.common.logging.ESLogger;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: capitol
 * Date: 2012-11-25
 * Time: 21:48
 */
public class RiverDatabase extends Database {
    private ESLogger logger;
    private int scale;
    private int rounding;

    public RiverDatabase(String url, String user, String password, ESLogger logger) {
        super(url, user, password);
        this.logger = logger;
    }

    public void setRounding(String rounding) {
        if ("ceiling".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_CEILING;
        } else if ("down".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_DOWN;
        } else if ("floor".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_FLOOR;
        } else if ("halfdown".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_HALF_DOWN;
        } else if ("halfeven".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_HALF_EVEN;
        } else if ("halfup".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_HALF_UP;
        } else if ("unnecessary".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_UNNECESSARY;
        } else if ("up".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_UP;
        }
    }

    public void setPrecision(int scale) {
        this.scale = scale;
    }

    public String getStartTime() {
        Connection conn = getConnection();
        PreparedStatement ps = null;
        ResultSet rs = null;
        String startTime = Long.toString(new Date().getTime() / 1000);

        try {
            ps = conn.prepareStatement("SELECT NOW()");
            rs = ps.executeQuery();
            if(rs.first())
                startTime = rs.getTimestamp(1).toString();
            rs.close();
            ps.close();
        }
        catch (SQLException ex) {
            logger.error("Exception while getting time", ex);
        }
        finally {
            if(rs != null) {
                try {
                    rs.close();
                } catch (SQLException ignore) { }
            }
            if(ps != null) {
                try {
                    ps.close();
                } catch (SQLException ignore) { }
            }
        }
        return startTime;
    }

    public int pushRowsToListener(String sql, RowListener pipe) {
        Connection conn = getConnection();
        PreparedStatement ps = null;
        ResultSet rs = null;
        int numberOfRows = 0;

        try {
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                numberOfRows++;
                Map<String, Object> row = new HashMap<String, Object>();
                ResultSetMetaData metadata = rs.getMetaData();
                int columns = metadata.getColumnCount();
                for (int i = 1; i <= columns; i++) {
                    String name = metadata.getColumnLabel(i);
                    row.put(name, SQLUtil.parseType(metadata.getColumnType(i), rs, i, logger, scale, rounding));
                }

                pipe.row(IndexOperation.INDEX, "", "", row);
            }
        }
        catch (SQLException ex) {
            logger.error("Exception while loading data", ex);
        } catch(IOException ex) {
            logger.error("Exception while loading data", ex);
        } finally {
            if(rs != null) {
                try {
                    rs.close();
                } catch (SQLException ignore) { }
            }
            if(ps != null) {
                try {
                    ps.close();
                } catch (SQLException ignore) { }
            }
        }
        return numberOfRows;
    }

    public int pushDeletesToListener(String sql, RowListener pipe) {
        Connection conn = getConnection();
        PreparedStatement ps = null;
        ResultSet rs = null;
        int numberOfDeletes = 0;

        try {
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                numberOfDeletes++;
                Map<String, Object> row = new HashMap<String, Object>();
                row.put("_id", rs.getString("_id"));

                pipe.row(IndexOperation.INDEX, "", "", row);
            }
        }
        catch (SQLException ex) {
            logger.error("Exception while reading deletes", ex);
        } catch(IOException ex) {
            logger.error("Exception while reading deletes", ex);
        } finally {
            if(rs != null) {
                try {
                    rs.close();
                } catch (SQLException ignore) { }
            }
            if(ps != null) {
                try {
                    ps.close();
                } catch (SQLException ignore) { }
            }
        }
        return numberOfDeletes;
    }
}
