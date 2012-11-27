package org.elasticsearch.river.jdbc.PipeParts;

import org.elasticsearch.river.jdbc.IndexOperation;
import org.elasticsearch.river.jdbc.RowListener;
import org.elasticsearch.river.jdbc.ValueSet;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: capitol
 * Date: 2012-11-25
 * Time: 23:29
 */
public class RowTransformer implements RowListener {
    private RowListener next;
    private char delimiter;

    public RowTransformer(RowListener next, char delimiter) {

        this.next = next;
        this.delimiter = delimiter;
    }

    @Override
    public void row(IndexOperation operation, String type, String id, Map<String, Object> row) throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        for(Map.Entry<String, Object> e : row.entrySet())
            merge(map, e.getKey(), e.getValue(), delimiter);

        next.row(operation, type, id, map);
    }

    @Override
    public void flush() {
        next.flush();
    }

    /**
     * Merge key/value pair to a map holding a JSON object. The key consists of
     * a path pointing to the value position in the JSON object. The key,
     * representing a path, is divided into head/tail. The recursion terminates
     * if there is only a head and no tail. In this case, the value is added as
     * a tuple to the map. If the head key exists, the merge process is
     * continued by following the path represented by the key. If the path does
     * not exist, a new map is created. A conflict arises if there is no map at
     * a head key position. Then, the prefix given in the path is considered
     * illegal.
     *
     * @param map the map for the JSON object
     * @param key the key
     * @param value the value
     */
    protected static void merge(Map<String, Object> map, String key, Object value, char delimiter) {
        int i = key.indexOf(delimiter);
        if (i <= 0) {
            map.put(key, new ValueSet(map.get(key), value));
        } else {
            String p = key.substring(0, i);
            String q = key.substring(i + 1);
            if (map.containsKey(p)) {
                Object o = map.get(p);
                if (o instanceof Map) {
                    merge((Map<String, Object>) o, q, value, delimiter);
                } else {
                    throw new IllegalArgumentException("illegal prefix: " + p);
                }
            } else {
                Map<String, Object> m = new HashMap<String, Object>();
                map.put(p, m);
                merge(m, q, value, delimiter);
            }
        }
    }
}
