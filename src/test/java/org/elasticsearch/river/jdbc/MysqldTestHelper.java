package org.elasticsearch.river.jdbc;

import com.mysql.management.MysqldResource;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
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

    private MysqldResource mysqldResource;
    private int mysqlPort;

    public void loadTables(String file, String url, String username, String password) throws IOException, SQLException {
        Connection connection = DriverManager.getConnection(url, username, password);

        ScriptRunner sr = new ScriptRunner(connection, true, true);

        sr.runScript(new FileReader(file));
    }

    protected String trimDelimiter(String input) {

        if(!word(1, input).equalsIgnoreCase("delimiter"))
            return input;

        String delimiter = word(2, input);

        String[] parts = input.split(delimiter);

        return parts[1];
    }

    private String word(int index, String input) {
        String[] parts = input.split("\\s");

        if(index >= parts.length)
            return "";

        return parts[index];
    }


    public void startMysql() throws IOException {
        Random random = new Random();
        mysqlPort = 10000 + random.nextInt(10000);
        File baseDir = File.createTempFile("test", "mysql");
        baseDir.delete();
        mysqldResource = new MysqldResource(baseDir);
        Map<String, String> options = new HashMap<String, String>();
        options.put("port", Integer.toString(mysqlPort));
        String threadName = "Test MySQL";
        mysqldResource.start(threadName, options);
    }

    public int getMysqlPort() {
        return mysqlPort;
    }

    public String getUrl() {
        return "jdbc:mysql://localhost:" + mysqlPort + "/";
    }

    public void tearDown() {
        if (mysqldResource != null) {
            mysqldResource.shutdown();
        }
        mysqldResource = null;
    }
}
