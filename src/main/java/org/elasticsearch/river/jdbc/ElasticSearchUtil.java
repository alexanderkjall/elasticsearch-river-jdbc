package org.elasticsearch.river.jdbc;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.river.RiverName;

import java.io.IOException;
import java.util.Date;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created with IntelliJ IDEA.
 * User: capitol
 * Date: 2012-11-25
 * Time: 22:45
 */
public class ElasticSearchUtil {


    public static void saveStatus(Date creationDate, Client client, String riverIndexName, RiverName riverName, long version, String digest, String status, long rows, String startTime) throws IOException {
        // save state to _custom
        XContentBuilder builder = jsonBuilder();
        builder.startObject().startObject("jdbc");
        if (creationDate != null) {
            builder.field("created", creationDate);
        }
        builder.field("version", version);
        if (digest != null){
            builder.field("digest", digest);
        }
        builder.field("status", status);
        builder.field("rows_processed", rows);
        builder.field("start_time", startTime);
        builder.endObject().endObject();
        client.prepareBulk().add(indexRequest(riverIndexName).type(riverName.name()).id("_custom").source(builder)).execute().actionGet();

    }
}
