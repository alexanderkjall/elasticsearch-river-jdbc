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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.*;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class JDBCRiver extends AbstractRiverComponent implements River {

    private final Client client;
    private final String riverIndexName;
    private final String indexName;
    private final String typeName;
    private final int bulkSize;
    private final TimeValue poll;
    private final String url;
    private final String user;
    private final String password;
    private final String sql;
    private final boolean rivertable;
    private volatile Thread thread;
    private AtomicBoolean closed;
    private Date creationDate;
    private RiverDatabase rdb;

    @Inject
    public JDBCRiver(RiverName riverName, RiverSettings settings,
                     @RiverIndexName String riverIndexName, Client client) {
        super(riverName, settings);
        this.riverIndexName = riverIndexName;
        this.client = client;
        closed = new AtomicBoolean(false);

        int scale;
        String rounding;
        boolean versioning;
        if (settings.settings().containsKey("jdbc")) {
            Map<String, Object> jdbcSettings = (Map<String, Object>) settings.settings().get("jdbc");
            poll = XContentMapValues.nodeTimeValue(jdbcSettings.get("poll"), TimeValue.timeValueMinutes(60));
            url = XContentMapValues.nodeStringValue(jdbcSettings.get("url"), null);
            user = XContentMapValues.nodeStringValue(jdbcSettings.get("user"), null);
            password = XContentMapValues.nodeStringValue(jdbcSettings.get("password"), null);
            sql = XContentMapValues.nodeStringValue(jdbcSettings.get("sql"), null);
            rivertable = XContentMapValues.nodeBooleanValue(jdbcSettings.get("rivertable"), false);
            interval = XContentMapValues.nodeTimeValue(jdbcSettings.get("interval"), TimeValue.timeValueMinutes(60));
            versioning = XContentMapValues.nodeBooleanValue(jdbcSettings.get("versioning"), true);
            rounding = XContentMapValues.nodeStringValue(jdbcSettings.get("rounding"), null);
            scale = XContentMapValues.nodeIntegerValue(jdbcSettings.get("scale"), 0);
        } else {
            poll = TimeValue.timeValueMinutes(60);
            url = null;
            user = null;
            password = null;
            sql = null;
            fetchsize = 0;
            rivertable = false;
            interval = TimeValue.timeValueMinutes(60);
            versioning = true;
            rounding = null;
            scale = 0;
        }

        TimeValue bulkTimeout;
        int maxBulkRequests;
        if (settings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
            indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), "jdbc");
            typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), "jdbc");
            bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
            maxBulkRequests = XContentMapValues.nodeIntegerValue(indexSettings.get("max_bulk_requests"), 30);
            if (indexSettings.containsKey("bulk_timeout")) {
                bulkTimeout = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(indexSettings.get("bulk_timeout"), "60s"), TimeValue.timeValueMillis(60000));
            } else {
                bulkTimeout = TimeValue.timeValueMillis(60000);
            }
        } else {
            indexName = "jdbc";
            typeName = "jdbc";
            bulkSize = 100;
            maxBulkRequests = 30;
            bulkTimeout = TimeValue.timeValueMillis(60000);
        }

        rdb = new RiverDatabase(url, user, password, logger);
    }

    @Override
    public void start() {
        logger.info("starting JDBC connector: URL [{}], sql [{}], river table [{}], indexing to [{}]/[{}], poll [{}]",
                url, sql, rivertable, indexName, typeName, poll);
        try {
            client.admin().indices().prepareCreate(indexName).execute().actionGet();
            creationDate = new Date();
        } catch (Exception e) {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                creationDate = null;
                // that's fine
            } else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
                // ok, not recovered yet..., lets start indexing and hope we recover by the first bulk
            } else {
                logger.warn("failed to create index [{}], disabling river...", e, indexName);
                return;
            }
        }

        Runnable toRun = new JDBCConnector(rdb, riverName, logger, sql, closed, client, riverIndexName, creationDate, poll, indexName, typeName, bulkSize);

        thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "JDBC connector").newThread(toRun);
        thread.start();
    }

    @Override
    public void close() {
        if (closed.get()) {
            return;
        }
        logger.info("closing JDBC river");
        thread.interrupt();
        closed.set(true);
    }
}
