package org.elasticsearch.river.jdbc;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * Created with IntelliJ IDEA.
 * User: capitol
 * Date: 2012-11-25
 * Time: 20:43
 */
public class Database {

    private ComboPooledDataSource cpds;

    public Database(String url, String user, String password) {
        cpds = new ComboPooledDataSource();
        //cpds.setDriverClass( "org.postgresql.Driver" ); //loads the jdbc driver
        cpds.setJdbcUrl(url);
        cpds.setUser(user);
        cpds.setPassword(password);

        // the settings below are optional -- c3p0 can work with defaults
        cpds.setMinPoolSize(5);
        cpds.setAcquireIncrement(5);
        cpds.setMaxPoolSize(20);
        // The DataSource cpds is now a fully configured and usable pooled DataSource
    }

    protected Connection getConnection() throws RuntimeException {
        try {
            return cpds.getConnection();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static void dropConnection(Connection conn, PreparedStatement... ps) throws RuntimeException {
        if (ps.length != 0) {
            for (PreparedStatement p : ps) {
                try {
                    p.close();
                }
                catch (Exception ignore) {
                }
            }
        }

        try {
            conn.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
