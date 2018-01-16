import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class TestEs5_new {
    public static void main(String[] args) {
        Settings settings = Settings.builder()
                .put("cluster.name", "smartlog_es_cluster").put("client.transport.sniff", true)
                .put("client.transport.ping_timeout", "30s")
                .put("client.transport.nodes_sampler_interval", "30s").build();
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("10.4.63.82", 9300)))
                .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("10.4.63.93", 9300)))
                .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("10.4.63.94", 9300)))
                .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("10.4.63.96", 9300)))
                .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("10.4.63.71", 9300)))
                .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("10.4.63.109", 9300)));
        //build destination settings
        Settings destiSettings = Settings.builder()
                .put("cluster.name", "smartlog_es_cluster").put("client.transport.sniff", true)
                .put("client.transport.ping_timeout", "30s")
                .put("client.transport.nodes_sampler_interval", "30s").build();
        TransportClient destiClient = new PreBuiltTransportClient(destiSettings);
        destiClient.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("10.4.63.82", 9300)))
                .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("10.4.63.93", 9300)))
                .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("10.4.63.94", 9300)))
                .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("10.4.63.96", 9300)))
                .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("10.4.63.71", 9300)))
                .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("10.4.63.109", 9300)));

        //取数据
        SearchResponse scrollResp = client.prepareSearch("smartlog_20171228")
                .setScroll(new TimeValue(60000)).setSize(1000).execute().actionGet();
        //build destination bulk
        BulkRequestBuilder bulk;
        ExecutorService executor = Executors.newFixedThreadPool(5);
        while (true) {
            bulk = destiClient.prepareBulk();
            final BulkRequestBuilder bulk_new = bulk;
            //存数据
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                System.out.println(hit.getSourceAsString().replace("2017-12-21", "2017-12-18"));
                IndexRequest req = destiClient.prepareIndex().setIndex("smartlog_20171218test")
                        .setType("all").setSource(hit.getSourceAsString().replace("2017-12-21", "2017-12-18")).request();
                bulk_new.add(req);
            }
            executor.execute(new Runnable() {
                public void run() {
                    bulk_new.execute();
                    System.out.println("插入成功");
                }
            });
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(60000)).execute().actionGet();
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }

    }
}
