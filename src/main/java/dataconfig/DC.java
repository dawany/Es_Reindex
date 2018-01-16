package dataconfig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import reindex.EsOperation;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

/**
 * 
 * @author
 * @desc : 配置文件对应类
 * @date:2015-9-6 上午10:23:01
 * @version :4.0
 */
public class DC implements Serializable {
	public static final long serialVersionUID = 1L;

	public static final Log log = LogFactory.getLog(DC.class);

	public static final String INPUT_ES_CLUSTER_NAME;
	public static final String OUTPUT_ES_CLUSTER_NAME;
	public static final String INPUT_NODE_IP_LIST;
	public static final String OUTPUT_NODE_IP_LIST;
	public static final String INPUTPERFIX;
	public static final String OUTPUTPREFIX;
	public static final String INPUTSUFFIX;
	public static final String OUTPUTSUFFIX;
	public static final int DAYNUM;
	public static final int NUMBER;
	public static final int THREADNUM;
	public static final String IP;
	public static final String BUSINESSLOG;
	public static final String STARTTIME;
	public static final String ENDTIME;
	public static final boolean FLAG;
	public static final boolean MESSAGEOUT;

	static {
		Properties pro = new Properties();
		try {
			pro.load(EsOperation.class.getClassLoader().getResourceAsStream("es.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		INPUT_ES_CLUSTER_NAME = pro.getProperty("INPUT_ES_CLUSTER_NAME");
		OUTPUT_ES_CLUSTER_NAME = pro.getProperty("OUTPUT_ES_CLUSTER_NAME");
		INPUT_NODE_IP_LIST = pro.getProperty("INPUT_NODE_IP_LIST");
		OUTPUT_NODE_IP_LIST = pro.getProperty("OUTPUT_NODE_IP_LIST");
		INPUTPERFIX = pro.getProperty("inputPrefix");
		OUTPUTPREFIX = pro.getProperty("outputPrefix");
		INPUTSUFFIX = pro.getProperty("inputSuffix");
		OUTPUTSUFFIX = pro.getProperty("outputSuffix");
		DAYNUM = Integer.parseInt(pro.getProperty("dayNum"));
		NUMBER = Integer.parseInt(pro.getProperty("number"));
		THREADNUM = Integer.parseInt(pro.getProperty("threadNum"));
		IP = pro.getProperty("ip");
		BUSINESSLOG = pro.getProperty("businesslog");
		STARTTIME = pro.getProperty("startTime");
		ENDTIME = pro.getProperty("endTime");
		FLAG = Boolean.parseBoolean(pro.getProperty("flag"));
		MESSAGEOUT = Boolean.parseBoolean(pro.getProperty("MessageOut"));
	}

	public static void main(String args[]) {
		System.out.println(DC.IP);
	}

}
