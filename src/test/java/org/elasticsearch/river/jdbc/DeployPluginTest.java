package org.elasticsearch.river.jdbc;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static junit.framework.Assert.assertEquals;

/**
 * Created with IntelliJ IDEA.
 * User: alexkjal
 * Date: 12/5/12
 * Time: 11:56 AM
 */
public class DeployPluginTest {

    @Test
    public void testDeployNew() throws IOException {

        ElasticSearchTestHelper esh = new ElasticSearchTestHelper();
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("http.port", "10000")
                .put("http.enabled", "true")
                .put("network.host", "localhost")
                .put("path.conf", new File(".").getCanonicalPath())
                .build();
        Node n = esh.buildNode("testDeployNew", settings);

        n.start();

        DefaultHttpClient httpClient = new DefaultHttpClient();

        HttpPut putRequest = new HttpPut("http://localhost:10000/_river/my_jdbc_river/_meta");

        StringEntity input = new StringEntity("{\n" +
                "    \"type\" : \"jdbc\",\n" +
                "    \"jdbc\" : {\n" +
                "        \"driver\" : \"com.mysql.jdbc.Driver\",\n" +
                "        \"url\" : \"jdbc:mysql://localhost:3306/test\",\n" +
                "        \"user\" : \"\",\n" +
                "        \"password\" : \"\",\n" +
                "        \"indexSql\" : \"select o.* from orders o, orders_log l where o._id=l._id and (l.op='i' or l.op='u')\",\n" +
                "        \"deleteSql\" : \"select _id from orders_log where op='d'\"\n" +
                "    }\n" +
                "}");
        input.setContentType("application/json");

        putRequest.setEntity(input);
        HttpResponse response = httpClient.execute(putRequest);

        assertEquals("check that we got a created status back", 201, response.getStatusLine().getStatusCode());
        System.out.println("response = " + response);

        n.stop();
    }
}
