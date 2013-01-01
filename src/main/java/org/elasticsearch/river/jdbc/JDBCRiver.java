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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.*;
import org.elasticsearch.river.jdbc.db.RiverDatabase;

import java.math.BigDecimal;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

public class JDBCRiver extends AbstractRiverComponent implements River {

    private final Client client;
    private volatile Thread thread;
    private AtomicBoolean closed;
    private Date creationDate;
    private RiverDatabase rdb;
    private RiverConfiguration rc;

    public enum IndexState {
        NEW,
        OLD
    }

    @Inject
    public JDBCRiver(RiverName riverName, RiverSettings settings, @RiverIndexName String riverIndexName, Client client) {
        super(riverName, settings);
        this.client = client;
        closed = new AtomicBoolean(false);

        rc = new RiverConfiguration();
        rc.getJDBCValues(settings);
        rc.getIndexValues(settings);
        rc.setRiverIndexName(riverIndexName);
        rc.setRiverName(riverName);

        rdb = new RiverDatabase(rc.getUrl(), rc.getUser(), rc.getPassword(), 1, BigDecimal.ROUND_CEILING);
    }

    @Override
    public void start() {
        logger.info("starting JDBC connector: URL [{}], indexSql [{}], deleteSql [{}], river table [{}], indexing to [{}]/[{}], poll [{}]",
                rc.getUrl(), rc.getIndexSql(), rc.getDeleteSql(), rc.getRiverName().getName(), rc.getRiverIndexName(), rc.getIndexName(), rc.getPoll());

        createIndex(rc.getRiverIndexName());
        IndexState state = createIndex(rc.getIndexName());

        Runnable toRun = new JDBCConnector(rdb, rc, closed, client, state);

        thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "JDBC connector").newThread(toRun);
        thread.start();
    }

    protected IndexState createIndex(String indexName) {
        logger.info("creating index: " + indexName);

        try {
            client.admin().indices().prepareCreate(indexName).execute().actionGet();
            //schedule full refresh
            return IndexState.NEW;
        } catch (Exception e) {
            Throwable t = ExceptionsHelper.unwrapCause(e);
            if (t instanceof IndexAlreadyExistsException || t instanceof ClusterBlockException) {
                return IndexState.OLD;
            }
            else {
                logger.error("failed to create index [{}], disabling river...", e, rc.getRiverIndexName());
                throw new RuntimeException("failed to create index [" + indexName + "], disabling river...", e);
            }
            
        }
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
