package org.elasticsearch.river.jdbc.PipeParts;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.river.jdbc.IndexOperation;
import org.elasticsearch.river.jdbc.RowListener;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: capitol
 * Date: 2012-11-26
 * Time: 20:59
 */
public class HashCreator implements RowListener {
    protected final ESLogger logger = Loggers.getLogger(getClass());

    private final static String DIGEST_ALGORITHM = "SHA-256";
    private final static String DIGEST_ENCODING = "UTF-8";

    private MessageDigest digest;
    private RowListener next;

    public HashCreator(RowListener next) {
        this.next = next;
        try {
            digest = MessageDigest.getInstance(DIGEST_ALGORITHM);
        } catch(NoSuchAlgorithmException e) {
            logger.error("Exception thrown: ", e);
        }
    }

    @Override
    public void row(IndexOperation operation, String type, String id, Map<String, Object> row) throws IOException {
        calculateHash(row, digest, DIGEST_ENCODING);
        byte[] hash = digest.digest();
        digest.reset();

        row.put("_content_hash", hash);

        next.row(operation, type, id, row);
    }

    @Override
    public void flush() {
        next.flush();
    }

    /**
     * Build JSON with the help of XContentBuilder.
     *
     * @param map the map holding the JSON object
     * @throws IOException
     */
    private static void calculateHash(Map<String, Object> map, MessageDigest digest, String encoding) throws IOException {
        for (String k : map.keySet()) {
            digest.update(k.getBytes(encoding));
            Object o = map.get(k);
            if (o instanceof Map) {
                calculateHash((Map<String, Object>) o, digest, encoding);
            }
            else
                digest.update(o.toString().getBytes(encoding));
        }
    }

}
