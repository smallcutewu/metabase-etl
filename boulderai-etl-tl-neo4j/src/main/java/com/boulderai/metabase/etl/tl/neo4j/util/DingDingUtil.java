package com.boulderai.metabase.etl.tl.neo4j.util;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import com.alibaba.fastjson.JSON;

/**
 * @ClassName: DingDingUtil
 * @Description: 钉钉工具
 * @author  df.l
 * @date 2023年06月18日
 * @Copyright boulderaitech.com
 */
@Slf4j
public class DingDingUtil {

	private static String dingDingUrl = "https://oapi.dingtalk.com/robot/send?access_token=86e60e0ae805435a2c2c0a71ffdb5071b890c4f9af737b14b1f1c506f8d49a45";

	private String dingDingSecret;

	public static void main(String[] args) {
				List<String> dingDingMobiles = new ArrayList<>();
		dingDingMobiles.add("138550779999");
		sendDingDingMsg("neo4j  @138550779999",dingDingMobiles);
	}





	public static void sendDingDingMsg(String content,List<String> dingDingMobiles) {
//		List<String> dingDingMobiles = new ArrayList<>();
//		dingDingMobiles.add("15355077922");
		//获取系统时间戳
		Long timestamp = System.currentTimeMillis();
		String sign = "sign";
		//钉钉机器人地址（配置机器人的webhook）
		String dingUrl = dingDingUrl + "&timestamp=" + timestamp + "&sign=" + sign;
		try {
			//通知具体人
			boolean isAtAll = false;
			//通知具体人的手机号码列表
			//组装请求内容
			String reqStr = buildReqStr(content, isAtAll, dingDingMobiles);
			//推送消息（http请求）
			String result = HttpUtil.sendHttpsPost(dingUrl, reqStr);
			log.info("钉钉群发消息返回：result{},,参数：reqStr{}", result, reqStr);
		} catch (Exception e) {
			e.printStackTrace();
			log.info("钉钉群发消息失败：content{}", content);
		}
	}

	/**
	 * 组装请求结构
	 */
	private static String buildReqStr(String content, boolean isAtAll, List<String> mobileList) {
		//消息内容
		HashMap<String, String> contentMap = new HashMap<>();
		contentMap.put("content", content);
		//通知人
		HashMap<String, Object> atMap = new HashMap<>();
		//1.是否通知所有人
		atMap.put("isAtAll", isAtAll);
		//2.通知具体人的手机号码列表
		atMap.put("atMobiles", mobileList);
		HashMap<String, Object> reqMap = new HashMap<>();
		reqMap.put("msgtype", "text");
		reqMap.put("text", contentMap);
		reqMap.put("at", atMap);
		return JSON.toJSONString(reqMap);
	}
}

