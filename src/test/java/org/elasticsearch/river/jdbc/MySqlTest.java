/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.river.jdbc;

import com.mysql.management.MysqldResource;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.*;

import static org.testng.Assert.assertEquals;

public class MySqlTest {
    private MysqldResource mysqldResource;
    private int mysqlPort;

    private String url;
    private String username;
    private String password;

    private SQLService service;
    private Connection connection;
    private PreparedStatement statement;

    @BeforeClass
    public void setup() throws IOException, ClassNotFoundException, SQLException {
        startMysql();

        url = "jdbc:mysql://localhost:" + mysqlPort + "/test";
        username = "";
        password = "";

        loadTables(new File (".").getCanonicalPath() + "/mysql-demo.sql");
    }

    private void loadTables(String file) throws IOException, ClassNotFoundException, SQLException {
        String content = FileUtils.readFileToString(new File(file));

        String[] statements = content.split(";");

        Connection connection = DriverManager.getConnection( url, username, password );

        for(String statement : statements) {
            if(statement.trim().length() != 0) {
                PreparedStatement sqlStatement = connection.prepareStatement(statement);
                sqlStatement.executeUpdate();
            }
        }
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

    private Action getDefaultAction() {
        return new DefaultAction() {

            @Override
            public void index(String index, String type, String id, long version, XContentBuilder builder) throws IOException {
                System.err.println("index=" + index + " type=" + type + " id=" + id + " builder=" + builder.string());
            }
        };
    }

    @AfterClass
    public void tearDown() {
        if (mysqldResource != null) {
            mysqldResource.shutdown();
        }
        mysqldResource = null;
    }

    private void setupSql(String sql, List<Object> params) throws SQLException {
        service = new SQLService();
        connection = service.getConnection(url, username, password, true);
        statement = service.prepareStatement(connection, sql);
        service.bind(statement, params);
    }

    @Test
    public void testStarQuery() throws IOException, SQLException, NoSuchAlgorithmException {
        String sql = "select * from orders";
        List<Object> params = new ArrayList<Object>();
        int fetchSize = 0;
        Action listener = getDefaultAction();

        setupSql(sql, params);

        ResultSet results = service.execute(statement, fetchSize);
        Merger merger = new Merger(listener, 1L);
        long rows = 0L;
        while (service.nextRow(results, merger)) {
            rows++;
        }
        assertEquals(rows, 5, "checking number of rows returned from the merger");

        close(merger, service, results, statement, connection);
    }

    @Test
    public void testBill() throws SQLException, IOException, NoSuchAlgorithmException {
        String sql = "select products.name as \"product.name\", orders.customer as \"product.customer.name\", orders.quantity * products.price as \"product.customer.bill\" from products, orders where products.name = orders.product ";
        List<Object> params = new ArrayList<Object>();
        int fetchSize = 0;
        Action listener = getDefaultAction();

        setupSql(sql, params);

        ResultSet results = service.execute(statement, fetchSize);
        Merger merger = new Merger(listener, 1L);
        long rows = 0L;
        while (service.nextRow(results, merger)) {
            rows++;
        }
        assertEquals(rows, 5, "checking number of rows returned from the merger");

        close(merger, service, results, statement, connection);
    }
    
    @Test
    public void testRelations() throws IOException, NoSuchAlgorithmException, SQLException {
        String sql = "select \"relations\" as \"_index\", orders.customer as \"_id\", orders.customer as \"contact.customer\", employees.name as \"contact.employee\" from orders left join employees on employees.department = orders.department";
        List<Object> params = new ArrayList<Object>();
        int fetchSize = 0;
        Action listener = getDefaultAction();

        setupSql(sql, params);

        ResultSet results = service.execute(statement, fetchSize);
        Merger merger = new Merger(listener, 1L);
        long rows = 0L;
        while (service.nextRow(results, merger)) {
            rows++;
        }
        assertEquals(rows, 11, "checking number of rows returned from the merger");

        close(merger, service, results, statement, connection);
    }
    
    @Test
    public void testHighBills() throws SQLException, IOException, NoSuchAlgorithmException {
        String sql = "select products.name as \"product.name\", orders.customer as \"product.customer.name\", orders.quantity * products.price as \"product.customer.bill\" from products, orders where products.name = orders.product and orders.quantity * products.price > ?";
        List<Object> params = new ArrayList<Object>();
        params.add(5.0);
        int fetchSize = 0;
        Action listener = getDefaultAction();

        setupSql(sql, params);

        ResultSet results = service.execute(statement, fetchSize);
        Merger merger = new Merger(listener, 1L);
        long rows = 0L;
        while (service.nextRow(results, merger)) {
            rows++;
        }
        assertEquals(rows, 0, "checking number of rows returned from the merger");

        close(merger, service, results, statement, connection);
    }

    @Test
    public void testTimePeriod() throws SQLException, IOException, NoSuchAlgorithmException {
        String sql = "select products.name as \"product.name\", orders.customer as \"product.customer.name\", orders.quantity * products.price as \"product.customer.bill\" from products, orders where products.name = orders.product and orders.created between ? - 14 and ?";
        List<Object> params = new ArrayList<Object>();
        params.add("2012-06-01");
        params.add("$now");
        int fetchSize = 0;
        Action listener = getDefaultAction();

        setupSql(sql, params);

        ResultSet results = service.execute(statement, fetchSize);
        Merger merger = new Merger(listener, 1L);
        long rows = 0L;
        while (service.nextRow(results, merger)) {
            rows++;
        }
        assertEquals(rows, 2, "checking number of rows returned from the merger");

        close(merger, service, results, statement, connection);
    }

    private void close(Merger merger, SQLService service, ResultSet results, PreparedStatement statement, Connection connection) throws IOException, SQLException {
        merger.close();
        service.close(results);
        service.close(statement);
        service.close(connection);
    }

}
