package reindex;

import dataconfig.DC;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import util.TimeUtil;

/**
 * @Description: 从es的索引中取数据插入到另外的索引中，用来给smartglog导数据，可以开多进程提高效率；
 * @Author: ydw
 * @Date: Created in 19:35 2017/12/27
 * @Modified by () on ().
 */
public class Task {
    public static final Log log = LogFactory.getLog(Task.class);

    public static void main(String[] args) {
        log.info("程序启动");

        final EsOperation esOperation = new EsOperation();
        final TransportClient client = esOperation.makeES(DC.INPUT_ES_CLUSTER_NAME, DC.INPUT_NODE_IP_LIST);
        final TransportClient destiClient = esOperation.makeES(DC.OUTPUT_ES_CLUSTER_NAME, DC.OUTPUT_NODE_IP_LIST);

        if (DC.INPUTSUFFIX == null || DC.INPUTSUFFIX.equals("") || DC.OUTPUTSUFFIX == null || DC.OUTPUTSUFFIX.equals("")) {
            log.info("请检查配置文件！");
        } else {
            //执行自行定义的插入方式
            String[] indexInArray = DC.INPUTSUFFIX.split(";");
            String[] indexOutArray = DC.OUTPUTSUFFIX.split(";");
            for (int i = 0; i < indexInArray.length; i++) {
                for (int j = 0; j < indexOutArray.length; j++) {
                    mainMethod(esOperation, client, destiClient, indexInArray[i], indexOutArray[j]);
                }
            }
        }
    }

    public static void mainMethod(EsOperation esOperation, TransportClient client, TransportClient destiClient, String indexIn, String indexOut) {
        System.out.println(TimeUtil.getSystemTime() + "--------------开始");
        SearchResponse searchResponse = esOperation.dataFilter(client, DC.INPUTPERFIX.concat(indexIn), DC.NUMBER, DC.IP, DC.BUSINESSLOG, indexIn, DC.STARTTIME, DC.ENDTIME);
        if (DC.FLAG) {
            esOperation.indexDeleter(destiClient, indexOut);
            esOperation.updateIndexMapping(destiClient, indexOut);
        }
        esOperation.dataChangerAndInsert(searchResponse, client, destiClient, indexIn, indexOut);
        System.out.println(TimeUtil.getSystemTime() + "--------------此索引导入完毕");
    }


}
