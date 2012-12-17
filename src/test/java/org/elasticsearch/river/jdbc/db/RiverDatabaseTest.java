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
package org.elasticsearch.river.jdbc.db;

import junit.framework.Assert;
import org.elasticsearch.river.jdbc.IndexOperation;
import org.elasticsearch.river.jdbc.MysqldTestHelper;
import org.elasticsearch.river.jdbc.RowListenerCollector;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

public class RiverDatabaseTest {

    private static MysqldTestHelper db;

    @BeforeClass
    public static void setup() throws IOException, SQLException {
        db = new MysqldTestHelper();
        db.startMysql();
        db.loadTables(new File(".").getCanonicalPath() + "/mysql-demo.sql", db.getUrl(), "root", "");
    }

    @AfterClass
    public static void tearDown() {
        db.tearDown();
    }

    @Test
    public void testGetTime() {
        RiverDatabase instance = new RiverDatabase(db.getUrl(), "root", "", 1, BigDecimal.ROUND_UP);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        String expResult = sdf.format(new Date());

        String result = instance.getTime();

        assertEquals("check that the time we get from the server is in sync", expResult, result.substring(0, 16));
    }

    @Test
    public void testPushRowsToListener() {
        RiverDatabase instance = new RiverDatabase(db.getUrl() + "test", "root", "", 1, BigDecimal.ROUND_UP);

        RowListenerCollector collector = new RowListenerCollector();

        instance.pushRowsToListener("select * from orders", collector);

        int expResult = 5;

        assertEquals("check number of updates", expResult, collector.getResults().size());
        Assert.assertEquals("check that we send INDEX operations", IndexOperation.INDEX, collector.getLastOp());
    }

    @Test
    public void testPushDeletesToListener() {
        RiverDatabase instance = new RiverDatabase(db.getUrl() + "test", "root", "", 1, BigDecimal.ROUND_UP);

        RowListenerCollector collector = new RowListenerCollector();

        instance.pushDeletesToListener("select * from orders", collector);

        int expResult = 5;

        assertEquals("check number of updates", expResult, collector.getResults().size());
        assertNotNull("check that we send _id", collector.getResults().get(0).get("_id"));
        assertEquals("check that we only send _id", 1, collector.getResults().get(0).size());
        assertEquals("check that we send DELETE operations", IndexOperation.DELETE, collector.getLastOp());
    }
}
