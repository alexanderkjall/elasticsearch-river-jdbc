package org.elasticsearch.river.jdbc;

import com.mysql.management.MysqldResource;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: alexkjal
 * Date: 12/4/12
 * Time: 6:04 PM
 */
public class MysqldTestHelper {

    private static MysqldResource mysqldResource;
    private static int mysqlPort;

    private static String url;
    private static String username;
    private static String password;

    private Connection connection;
    private PreparedStatement statement;

    private static void loadTables(String file) throws IOException, ClassNotFoundException, SQLException {
        String content = FileUtils.readFileToString(new File(file));

        String[] statements = content.split(";");

        Connection connection = DriverManager.getConnection(url, username, password);

        for(String statement : statements) {
            if(statement.trim().length() != 0) {
                PreparedStatement sqlStatement = connection.prepareStatement(statement);
                sqlStatement.executeUpdate();
            }
        }
    }

    public static MysqldResource startMysql() throws IOException {
        Random random = new Random();
        mysqlPort = 10000 + random.nextInt(10000);
        File baseDir = File.createTempFile("test", "mysql");
        baseDir.delete();
        mysqldResource = new MysqldResource(baseDir);
        Map<String, String> options = new HashMap<String, String>();
        options.put("port", Integer.toString(mysqlPort));
        String threadName = "Test MySQL";
        mysqldResource.start(threadName, options);

        return mysqldResource;
    }

}
