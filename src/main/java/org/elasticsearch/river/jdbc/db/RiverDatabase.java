package org.elasticsearch.river.jdbc.db;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.river.jdbc.IndexOperation;
import org.elasticsearch.river.jdbc.RowListener;

import java.io.IOException;
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

    protected final ESLogger logger = Loggers.getLogger(getClass());

    private int scale;
    private int rounding;

    public RiverDatabase(String url, String user, String password, int scale, int rounding) {
        super(url, user, password);
        this.scale = scale;
        this.rounding = rounding;
    }

    public String getTime() {
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
                    row.put(name, SQLUtil.parseType(metadata.getColumnType(i), rs, i, scale, rounding));
                }

                pipe.row(IndexOperation.INDEX, "", row);
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

                pipe.row(IndexOperation.DELETE, "", row);
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
