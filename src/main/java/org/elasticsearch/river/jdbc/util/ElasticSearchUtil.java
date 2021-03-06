package org.elasticsearch.river.jdbc.util;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: capitol
 * Date: 2012-11-25
 * Time: 22:45
 */
public class ElasticSearchUtil {


    public static Map<String, Object> createStatusMap(String status, long totalRows, long rowsPerSecond, String startTime) {
        Map<String, Object> root = new HashMap<String, Object>();
        Map<String, Object> lvl1 = new HashMap<String, Object>();
        
        root.put("jdbc", lvl1);
        lvl1.put("created", new Date());

        lvl1.put("status", status);
        lvl1.put("rows_processed", totalRows);
        lvl1.put("rows_per_second", rowsPerSecond);
        lvl1.put("start_time", startTime);

        return root;
    }
}
