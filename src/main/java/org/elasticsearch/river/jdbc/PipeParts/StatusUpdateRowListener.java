package org.elasticsearch.river.jdbc.PipeParts;

import org.elasticsearch.river.jdbc.*;
import org.elasticsearch.river.jdbc.util.DateUtil;
import org.elasticsearch.river.jdbc.util.ElasticSearchUtil;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: capitol
 * Date: 2012-11-25
 * Time: 22:36
 */
public class StatusUpdateRowListener implements RowListener {
    private RowListener next;
    private RowListener riverEndPoint;
    private Date startTime;
    private long rows;
    private Date lastTick;

    public StatusUpdateRowListener(RowListener next, RowListener riverEndPoint) {

        this.next = next;
        this.riverEndPoint = riverEndPoint;
        startTime = new Date();
        lastTick = new Date();
    }

    @Override
    public void row(IndexOperation operation, String id, Map<String, Object> row) throws IOException {
        rows++;
        if (rows % 1000 == 0) {
            Date currentTick = new Date();

            long time = currentTick.getTime() - lastTick.getTime();
            if(time == 0)
                time = 1;

            riverEndPoint.row(operation, "_custom", ElasticSearchUtil.createStatusMap("running", rows, 1000000 / time, DateUtil.formatDateISO(startTime)));

            lastTick = currentTick;
        }

        next.row(operation, id, row);
    }

    @Override
    public void refresh() {
        rows = 0;
        startTime = new Date();
        next.refresh();
    }
}
