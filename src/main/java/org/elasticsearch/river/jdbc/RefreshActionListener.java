package org.elasticsearch.river.jdbc;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: capitol
 * Date: 2012-12-25
 * Time: 09:10
 */
public class RefreshActionListener implements ActionListener<RefreshResponse> {
    protected final ESLogger logger = Loggers.getLogger(getClass());
    private Date date;

    public RefreshActionListener() {
        date = new Date();
    }

    @Override
    public void onResponse(RefreshResponse refreshResponse) {
        if (refreshResponse.failedShards() > 0) {
            logger.error("bulk request from {} has failures: {} shards out of {} failed", date, refreshResponse.failedShards(), refreshResponse.totalShards());
            for(ShardOperationFailedException e : refreshResponse.getShardFailures())
                logger.error("shard failure", e);
        } else {
            logger.info("refresh request from {} succeeded ({} millis, {} docs, {} docs sent)", date, refreshResponse.successfulShards(), refreshResponse.totalShards() );
        }
    }

    @Override
    public void onFailure(Throwable throwable) {
        logger.error("total refresh failure", throwable);
    }
}
