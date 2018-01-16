import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @Description:
 * @Author: ydw
 * @Date: Created in 13:04 2017/12/28
 * @Modified by () on ().
 */
public class EsFilterByIPandExport {
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

        SearchResponse response = client.prepareSearch("smartlog_20171229")
                .setTypes()
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termsQuery("sourceIp", "10.7.6.119","10.4.63.112","10.7.6.116"))                 // Query 匹配查询
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(10000).setScroll(new TimeValue(600000))
                //.setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
                //.setFrom(0).setSize(60).setExplain(true)
                .get();
        // MatchAll on the whole cluster with all default options
        // SearchResponse response = client.prepareSearch().get();
        String scrollid = response.getScrollId();
        try {
            //把导出的结果以JSON的格式写到文件里
            BufferedWriter out = new BufferedWriter(new FileWriter("E:\\神州泰岳\\Ultrapower\\smartlog_20171229Filter1.txt", true)); //文件位置

            //每次返回数据10000条。一直循环查询直到所有的数据都查询出来
            while (true) {
                SearchResponse response2 = client.prepareSearchScroll(scrollid).setScroll(new TimeValue(1000000))
                        .execute().actionGet();
                SearchHits searchHit = response2.getHits();
                //再次查询不到数据时跳出循环
                if (searchHit.getHits().length == 0) {
                    break;
                }
                System.out.println("查询数量 ：" + searchHit.getHits().length);
                for (int i = 0; i < searchHit.getHits().length; i++) {
                    String json = searchHit.getHits()[i].getSourceAsString();
                    out.write(json);
                    out.write("\r\n");
                }
            }
            System.out.println("查询结束");
            out.close();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
