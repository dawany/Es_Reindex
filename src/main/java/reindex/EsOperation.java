package reindex;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import util.StringUtil;
import util.TimeUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class EsOperation {
    private static Log log = LogFactory.getLog(EsOperation.class);

    /**
     * @Description: 配置es集群名及相关配置;配置ES集群ip和端口
     * @Author:dell
     * @Date: Created in 19:12 2018/1/10
     */
    public TransportClient makeES(String clusterName, String ipList) {
        Settings settings = Settings.builder()
                .put("cluster.name", clusterName).put("client.transport.sniff", true)
                .put("client.transport.ping_timeout", "30s")
                .put("client.transport.nodes_sampler_interval", "30s").build();
        String[] inputNodeIpArr = StringUtil.getIPandPort(ipList);
        TransportClient transportClient = new PreBuiltTransportClient(settings);
        for (int i = 0; i < inputNodeIpArr.length; i++) {
            transportClient.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(StringUtil.getIP(inputNodeIpArr[i]), StringUtil.getPort(inputNodeIpArr[i]))));
        }
        log.info("读取ES集群信息完毕");
        System.out.println("-----------------------读取ES集群信息完毕-----------------------");
        return transportClient;
    }

    /**
     * @Description: 定义匹配条件, 将符合条件的记录取出来
     * @Author:dell
     * @Date: Created in 19:12 2018/1/10
     */
    public SearchResponse dataFilter(TransportClient transportClient, String indexName, int number, String ip, String businesslog, String inputSuffix, String startTime, String endTime) {
        // SearchResponse scrollResp = transportClient.prepareSearch(indexName).setScroll(new TimeValue(60000)).setSize(number).execute().actionGet();
//        SearchResponse scrollResp = transportClient.prepareSearch(indexName)
//                .setTypes()
//                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                //.setQuery(QueryBuilders.matchAllQuery())
//                //.setPostFilter(QueryBuilders.boolQuery().must(QueryBuilders.termsQuery("sourceIp", ip1, ip2, ip3, ip4))
//                .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termsQuery("sourceIp", ip1, ip2, ip3, ip4))                 // Query 匹配查询
//              //  .setFilter(FilterBuilders.termsQuery("sourceIp", ip1, ip2, ip3, ip4))
//                .setSize(number).setScroll(new TimeValue(600000))
//                .execute().actionGet();

        String[] ips = StringUtil.getArrays(ip);
        String[] logs = StringUtil.getArrays(businesslog);
        SearchResponse scrollResp = transportClient.prepareSearch(indexName)
        transportClient.
                .setTypes()
                //多条件搜索，只能使用一个setQuery;使用多个的话，只有最后一个setQuery生效
                .setQuery(QueryBuilders.boolQuery()
                        //多条件搜索，使用多个must增加多个限制条件
                        .must(QueryBuilders.termsQuery("sourceIp", ips))
                        .must(QueryBuilders.termsQuery("sourceTag", logs))
                        .must(QueryBuilders.rangeQuery("orgTime").gte(StringUtil.dateToStamp(inputSuffix, startTime)).lte(StringUtil.dateToStamp(inputSuffix, endTime))))
                .setSize(number).setScroll(new TimeValue(600000))
                .execute().actionGet();
        log.info("-----------------------定义过滤方式完毕-----------------------");
        System.out.println("-----------------------定义过滤方式完毕-----------------------");
        return scrollResp;
    }

    /**
     * @Description: 判断目标索引是否存在, 如果存在则删除索引，为插入数据做准备
     * @Author:dell
     * @Date: Created in 19:43 2018/1/10
     */
    public void indexDeleter(TransportClient destiClient, String newTime) {
        IndicesExistsRequest inExistsRequest = new IndicesExistsRequest(DC.OUTPUTPREFIX.concat(newTime));
        IndicesExistsResponse inExistsResponse = destiClient.admin().indices()
                .exists(inExistsRequest).actionGet();
        DeleteIndexResponse dResponse;
        if (inExistsResponse.isExists()) {
            try {
                dResponse = destiClient.admin().indices().prepareDelete(DC.OUTPUTPREFIX.concat(newTime))
                        .execute().actionGet();
                System.out.println("删除索引成功!" + DC.OUTPUTPREFIX.concat(newTime));
                log.info("删除索引成功!" + DC.OUTPUTPREFIX.concat(newTime));
            } catch (Exception e) {
                System.out.println("删除索引失败");
                log.info("删除索引失败");
                e.printStackTrace();
            }
        } else {
            System.out.println("索引不存在");
            log.info("索引不存在");
        }
        System.out.println("-----------------------处理删除索引完毕-----------------------");
    }

    /**
     * @Description: 处理es里的字段 获取某个字段的值
     * @Author:dell
     * @Date: Created in 20:25 2018/1/10
     */
    public static String getDataFromESbyField(String jsonStr, String key) {
        JSONObject json = JSON.parseObject(jsonStr);
        return json.getString(key);
    }

    /**
     * @Description: 创建索引，定义mapping
     * @Author:dell
     * @Date: Created in 13:00 2018/1/14
     */
    public void updateIndexMapping(TransportClient destiClient, String newTime) {
        String index = DC.OUTPUTPREFIX.concat(newTime);
        destiClient.admin().indices().prepareCreate(index).execute().actionGet();
        XContentBuilder mapping;
        try {
            mapping = jsonBuilder().startObject()
                    .startObject("all")    //必须是类型名
                    .startObject("properties")
                    .startObject("orgTime").field("type", "date").field("index", "not_analyzed").endObject()
                    .startObject("collectTime").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("time_hour").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("time_mday").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("time_minute").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("time_month").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("time_second").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("time_wday").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("time_week").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("time_year").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("time_zone").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("rollIndex").field("type", "long").field("index", "not_analyzed").endObject()
                    .startObject("lines").field("type", "long").field("index", "not_analyzed").endObject()
                    .endObject()
                    .endObject()
                    .endObject();
            PutMappingRequest mappingRequest = Requests.putMappingRequest(index).type("all").source(mapping);
            destiClient.admin().indices().putMapping(mappingRequest).actionGet();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("-----------------------处理定义索引完毕-----------------------");
    }


//smartlog代码
//    public JSONObject gen_new_mappings(){
//        JSONObject mappings = new JSONObject();
//        if(logRuleList != null){
//            if(logRuleList.size()>0){
//                for (int i = 0; i < logRuleList.size(); i++) {
//                    LogRuleBean logRuleBean = JSONObject.parseObject(logRuleList.get(i).toString(), LogRuleBean.class);
//                    List<LogRuleFieldVO> logRuleBeanFieldPick = getLogRuleFieldPick(logRuleBean);
//
//                    JSONObject mapping = new JSONObject();
//                    mapping.put("dynamic", true);
//                    JSONObject _all = new JSONObject();
//                    _all.put("enabled", false);
//                    mapping.put("_all",_all);
//
//                    JSONObject type_properties = new JSONObject();
//                    type_properties.put("type", "nested");
//                    type_properties.put("include_in_parent", true);
//                    type_properties.put("dynamic", true);
//
//                    JSONObject properties = this.gen_type_mapping(logRuleBeanFieldPick);
//                    type_properties.put("properties",properties);
//
//                    JSONObject type_mapping = this.Conf.getPublicFields();
//                    type_mapping.put(logRuleBean.getLogRuleName(), type_properties);
//                    mapping.put("properties", type_mapping);
//                    //Dynamic templates
//                    JSONArray dynamicTemplates = new JSONArray();
//                    JSONObject strTemplate = JSON.parseObject(STRTEMPLATE);
//                    dynamicTemplates.add(strTemplate);
//                    mapping.put("dynamic_templates", dynamicTemplates);
//                    mappings.put(logRuleBean.getLogRuleName(), mapping);
//                }
//            }
//        }
//        JSONObject unknow_mapping = this.get_unknow_mapping();
//        mappings.put("unknow", unknow_mapping);
//        return mappings;
//    }


    public void dataChangerAndInsert(SearchResponse scrollResp, TransportClient client, TransportClient destiClient, String oldTime, String newTime) {

        ExecutorService executor = Executors.newFixedThreadPool(DC.THREADNUM);
        System.out.println(DC.OUTPUTPREFIX.concat(newTime));
        while (true) {
            final BulkRequestBuilder bulk_new = destiClient.prepareBulk();
            //存数据
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                if (DC.MESSAGEOUT) {
                    System.out.println("原始数据>>>>>>>>>>>>>>>>>>" + hit.getSourceAsString());
                }
                String oldTimeString = getDataFromESbyField(hit.getSourceAsString(), "timeString");
                String timeString = oldTimeString.replaceAll(StringUtil.formalTime(oldTime), StringUtil.formalTime(newTime));

                String orgTime = getDataFromESbyField(hit.getSourceAsString(), "orgTime");
                String sourceType = getDataFromESbyField(hit.getSourceAsString(), "sourceType");
                String orgId = getDataFromESbyField(hit.getSourceAsString(), "orgId");
                String time_mday = getDataFromESbyField(hit.getSourceAsString(), "time_mday");
                String time_month = getDataFromESbyField(hit.getSourceAsString(), "time_month");
                String time_wday = getDataFromESbyField(hit.getSourceAsString(), "time_wday");
                String time_week = getDataFromESbyField(hit.getSourceAsString(), "time_week");
                String time_year = getDataFromESbyField(hit.getSourceAsString(), "time_year");
                String new_time_year = TimeUtil.getYear(StringUtil.dateToStamp(timeString));

                Map esOrgData = hit.getSource();

                String time_mdayValue = esOrgData.get("time_mday").toString().replaceAll(time_mday, TimeUtil.getDayInMonth(timeString));
                String time_monthValue = esOrgData.get("time_month").toString().replaceAll(time_month, TimeUtil.getMonthInYear(timeString));
                String time_wdayValue = esOrgData.get("time_wday").toString().replaceAll(time_wday, TimeUtil.getDayInWeek(timeString));
                String time_weekValue = esOrgData.get("time_week").toString().replaceAll(time_week, TimeUtil.getWeekInMonth(timeString));
                String time_yearValue = esOrgData.get("time_year").toString().replaceAll(time_year, new_time_year);

                String orgIdValue = esOrgData.get("orgId").toString().replaceAll(time_year, new_time_year);
                String orgDataValue = esOrgData.get("orgData").toString()
                        .replaceAll(StringUtil.formalTime(oldTime), StringUtil.formalTime(newTime))                //2017-01-09格式的数据替换0
                        .replaceAll(StringUtil.subTime(oldTime), StringUtil.subTime(newTime))                      //  170109格式的数据替换0
                        .replaceAll(StringUtil.formalTime2(oldTime), StringUtil.formalTime2(newTime))              //2018-1-10格式的数据替换
                        .replaceAll(orgTime, StringUtil.dateToStamp(timeString))                                   //修改时间戳
                        .replaceAll(StringUtil.changeToEN(oldTimeString), StringUtil.changeToEN(timeString))       //修改英文时间
                        .replaceAll(StringUtil.changeToEN1(oldTimeString), StringUtil.changeToEN(timeString))       //修改英文时间
                        .replaceAll(StringUtil.changeToEN3(oldTimeString), StringUtil.changeToEN(timeString))       //修改英文时间
                        .replaceAll(StringUtil.changeToEN4(oldTimeString), StringUtil.changeToEN4(timeString))       //修改英文时间Jan+4L2aDIJdp5pN1JXNiHvsrUD0zDaqlDDfFl/0tnphXpgmmhcygbgRY8e14IYbfH43m7+wGg7b
                        .replaceAll(time_year, new_time_year);
                String timeStringValue = esOrgData.get("timeString").toString()
                        .replaceAll(StringUtil.formalTime(oldTime), StringUtil.formalTime(newTime))                //2017-01-09格式的数据替换0
                        .replaceAll(StringUtil.subTime(oldTime), StringUtil.subTime(newTime))                      //  170109格式的数据替换0
                        .replaceAll(StringUtil.formalTime2(oldTime), StringUtil.formalTime2(newTime))              //2018-1-10格式的数据替换
                        .replaceAll(orgTime, StringUtil.dateToStamp(timeString))                                   //修改时间戳
                        .replaceAll(StringUtil.changeToEN(oldTimeString), StringUtil.changeToEN(timeString))       //修改英文时间
                        .replaceAll(StringUtil.changeToEN1(oldTimeString), StringUtil.changeToEN(timeString))       //修改英文时间
                        .replaceAll(StringUtil.changeToEN3(oldTimeString), StringUtil.changeToEN(timeString))       //修改英文时间
                        .replaceAll(time_year, new_time_year);
                String sourceModeValue = esOrgData.get("sourceMode").toString()
                        .replaceAll(StringUtil.formalTime(oldTime), StringUtil.formalTime(newTime))                //2017-01-09格式的数据替换0
                        .replaceAll(StringUtil.subTime(oldTime), StringUtil.subTime(newTime))                      //  170109格式的数据替换0
                        .replaceAll(StringUtil.formalTime2(oldTime), StringUtil.formalTime2(newTime))              //2018-1-10格式的数据替换
                        .replaceAll(orgTime, StringUtil.dateToStamp(timeString))                                   //修改时间戳
                        .replaceAll(StringUtil.changeToEN(oldTimeString), StringUtil.changeToEN(timeString))       //修改英文时间
                        .replaceAll(StringUtil.changeToEN1(oldTimeString), StringUtil.changeToEN(timeString))       //修改英文时间
                        .replaceAll(StringUtil.changeToEN3(oldTimeString), StringUtil.changeToEN(timeString))       //修改英文时间
                        .replaceAll(time_year, new_time_year);
                String orgTimeValue = esOrgData.get("orgTime").toString()
                        .replaceAll(StringUtil.formalTime(oldTime), StringUtil.formalTime(newTime))                //2017-01-09格式的数据替换0
                        .replaceAll(StringUtil.subTime(oldTime), StringUtil.subTime(newTime))                      //  170109格式的数据替换0
                        .replaceAll(StringUtil.formalTime2(oldTime), StringUtil.formalTime2(newTime))              //2018-1-10格式的数据替换
                        .replaceAll(orgTime, StringUtil.dateToStamp(timeString))                                   //修改时间戳
                        .replaceAll(StringUtil.changeToEN(oldTimeString), StringUtil.changeToEN(timeString))       //修改英文时间
                        .replaceAll(StringUtil.changeToEN1(oldTimeString), StringUtil.changeToEN(timeString))       //修改英文时间
                        .replaceAll(StringUtil.changeToEN3(oldTimeString), StringUtil.changeToEN(timeString))       //修改英文时间
                        .replaceAll(time_year, new_time_year);

                esOrgData.put("orgData", orgDataValue);
                esOrgData.put("timeString", timeStringValue);
                esOrgData.put("sourceMode", sourceModeValue);
                esOrgData.put("orgId", orgIdValue);
                esOrgData.put("time_year", Long.parseLong(time_yearValue));
                esOrgData.put("orgTime", Long.parseLong(orgTimeValue));
                esOrgData.put("time_mday", Long.parseLong(time_mdayValue));
                esOrgData.put("time_month", Long.parseLong(time_monthValue));
                esOrgData.put("time_wday", Long.parseLong(time_wdayValue));
                esOrgData.put("time_week", Long.parseLong(time_weekValue));

                IndexRequest req = destiClient.prepareIndex(DC.OUTPUTPREFIX.concat(newTime), sourceType, orgId).setSource(esOrgData).request();
                if (DC.MESSAGEOUT) {
                    System.out.println("插入数据>>>>>>>>>>>>>>>>>>" + req.toString());
                }
                bulk_new.add(req);
            }
            //提交
            executor.execute(new Runnable() {
                public void run() {
                    bulk_new.execute();
                }
            });

            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(60000)).execute().actionGet();
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }
        //未测试
        client.close();
        destiClient.close();
    }

}
