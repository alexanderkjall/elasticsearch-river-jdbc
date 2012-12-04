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

import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

public class JDBCRiver extends AbstractRiverComponent implements River {

    private final Client client;
    private volatile Thread thread;
    private AtomicBoolean closed;
    private Date creationDate;
    private RiverDatabase rdb;
    private RiverConfiguration rc;

    @Inject
    public JDBCRiver(RiverName riverName, RiverSettings settings,
                     @RiverIndexName String riverIndexName, Client client) {
        super(riverName, settings);
        this.client = client;
        closed = new AtomicBoolean(false);

        rc = new RiverConfiguration();
        if (settings.settings().containsKey("jdbc"))
            rc.getJDBCValues(settings);

        if (settings.settings().containsKey("index"))
            rc.getIndexValues(settings);

        rdb = new RiverDatabase(rc.getUrl(), rc.getUser(), rc.getPassword(), logger);
    }

    @Override
    public void start() {
        logger.info("starting JDBC connector: URL [{}], sql [{}], river table [{}], indexing to [{}]/[{}], poll [{}]",
                rc.getUrl(), rc.getSql(), rc.getRiverName(), rc.getRiverIndexName(), rc.getTypeName(), rc.getPoll());
        try {
            client.admin().indices().prepareCreate(rc.getRiverIndexName()).execute().actionGet();
            creationDate = new Date();
        } catch (Exception e) {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                creationDate = null;
                // that's fine
            } else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
                // ok, not recovered yet..., lets start indexing and hope we recover by the first bulk
            } else {
                logger.warn("failed to create index [{}], disabling river...", e, rc.getRiverIndexName());
                return;
            }
        }

        Runnable toRun = new JDBCConnector(rdb, rc, logger, closed, client, creationDate);

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
