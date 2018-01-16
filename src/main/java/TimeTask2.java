import dataconfig.DC;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import util.StringUtil;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @Description:
 * @Author: ydw
 * @Date: Created in 19:35 2017/12/27
 * @Modified by () on ().
 */
public class TimeTask2 {
    private static Log log = LogFactory.getLog(TimeTask2.class);
    private static String INPUT_ES_CLUSTER_NAME;
    private static String OUTPUT_ES_CLUSTER_NAME;
    private static String INPUT_NODE_IP_LIST;
    private static String OUTPUT_NODE_IP_LIST;
    private static String INPUTPERFIX;
    private static String OUTPUTPREFIX;
    private static int dayNum;
    private static int number;
    private static int threadNum;
    private static String ip;

    static {
        Properties pro = new Properties();
        try {
            pro.load(reindex.EsOperation.class.getClassLoader().getResourceAsStream("es.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        INPUT_ES_CLUSTER_NAME = pro.getProperty("INPUT_ES_CLUSTER_NAME");
        OUTPUT_ES_CLUSTER_NAME = pro.getProperty("OUTPUT_ES_CLUSTER_NAME");
        INPUT_NODE_IP_LIST = pro.getProperty("INPUT_NODE_IP_LIST");
        OUTPUT_NODE_IP_LIST = pro.getProperty("OUTPUT_NODE_IP_LIST");
        INPUTPERFIX = pro.getProperty("inputPrefix");
        OUTPUTPREFIX = pro.getProperty("outputPrefix");
        dayNum = Integer.parseInt(pro.getProperty("dayNum"));
        number = Integer.parseInt(pro.getProperty("number"));
        threadNum = Integer.parseInt(pro.getProperty("threadNum"));
        ip = pro.getProperty("ip");

    }

    public static void main(String[] args) {
        log.info("程序启动");
        final reindex.EsOperation esOperation = new reindex.EsOperation();
        final TransportClient client = esOperation.makeES(INPUT_ES_CLUSTER_NAME, INPUT_NODE_IP_LIST);
        final TransportClient destiClient = esOperation.makeES(OUTPUT_ES_CLUSTER_NAME, OUTPUT_NODE_IP_LIST);

        System.out.println(INPUT_ES_CLUSTER_NAME);
        Runnable runnable = new Runnable() {
            public void run() {
                System.out.println("Hello !!我要开始插入了");
                SearchResponse scrollResp = esOperation.dataFilter(client, INPUTPERFIX.concat(StringUtil.getYesterday()), number, ip, DC.BUSINESSLOG, DC.INPUTSUFFIX, DC.STARTTIME, DC.ENDTIME);
                ExecutorService executor = Executors.newFixedThreadPool(threadNum);
                while (true) {
                    final BulkRequestBuilder bulk_new = destiClient.prepareBulk();
                    //存数据
                    for (SearchHit hit : scrollResp.getHits().getHits()) {
                        System.out.println("A原始数据>>>>>>>>>>>>>>>>>>" + hit.getSourceAsString());
                        System.out.println(OUTPUTPREFIX.concat(StringUtil.getOtherDay(dayNum)));
                        IndexRequest req = destiClient.prepareIndex().setIndex(OUTPUTPREFIX.concat(StringUtil.getOtherDay(dayNum)))
                                .setType("all").setSource(hit.getSourceAsString().replace(StringUtil.getYesterday2(), StringUtil.getOtherDay2(dayNum))).request();
                        System.out.println("插入数据>>>>>>>>>>>>>>>>>>" + req.toString());
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
        };
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        // 第二个参数为首次执行的延时时间，第三个参数为定时执行的间隔时间
        service.scheduleAtFixedRate(runnable, 5, 86400, TimeUnit.SECONDS);
    }
}
