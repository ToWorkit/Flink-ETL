package com.pyg.report.util;

import com.alibaba.fastjson.JSONObject;
import com.pyg.report.bean.ClickLog;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.Test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * 点击流日志模拟器
 */
public class ClickLogGenerator {
    private static Long[] channelID = new Long[]{1l,2l,3l,4l,5l,6l,7l,8l,9l,10l,11l,12l,13l,14l,15l,16l,17l,18l,19l,20l};//频道id集合
    private static Long[] categoryID = new Long[]{1l,2l,3l,4l,5l,6l,7l,8l,9l,10l,11l,12l,13l,14l,15l,16l,17l,18l,19l,20l};//产品类别id集合
    private static Long[] produceID = new Long[]{1l,2l,3l,4l,5l,6l,7l,8l,9l,10l,11l,12l,13l,14l,15l,16l,17l,18l,19l,20l};//产品id集合
    private static Long[] userID = new Long[]{1l,2l,3l,4l,5l,6l,7l,8l,9l,10l,11l,12l,13l,14l,15l,16l,17l,18l,19l,20l};//用户id集合

    /**
     * 地区
     */
    private static String[] contrys = new String[]{"china"};//地区-国家集合
    private static String[] provinces = new String[]{"HeNan","HeBeijing"};//地区-省集合
    private static String[] citys = new String[]{"ShiJiaZhuang","ZhengZhou", "LuoYang"};//地区-市集合

    /**
     *网络方式
     */
    private static String[] networks = new String[]{"电信","移动","联通"};

    /**
     * 来源方式
     */
    private static String[] sources = new String[]{"直接输入","百度跳转","360搜索跳转","必应跳转"};

    /**
     * 浏览器
     */
    private static String[] browser = new String[]{"火狐","qq浏览器","360浏览器","谷歌浏览器"};

    /**
     * 打开时间 离开时间
     */
    private static List<Long[]> usetimelog = producetimes();
    //获取时间
    public static List<Long[]> producetimes(){
        List<Long[]> usetimelog = new ArrayList<Long[]>();
        for(int i=0;i<100;i++){
            Long [] timesarray = gettimes("2018-12-12 24:60:60:000");
            usetimelog.add(timesarray);
        }
        return usetimelog;
    }

    private static Long [] gettimes(String time){
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");
        try {
            Date date = dateFormat.parse(time);
            long timetemp = date.getTime();
            Random random = new Random();
            int randomint = random.nextInt(10);
            long starttime = timetemp - randomint*3600*1000;
            long endtime = starttime + randomint*3600*1000;
            return new Long[]{starttime,endtime};
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return new Long[]{0l,0l};
    }


    /**
     * 模拟发送Http请求到上报服务系统
     * @param url
     * @param json
     */
    public static void send(String url, String json) {
        try {
            CloseableHttpClient httpClient = HttpClientBuilder.create().build();
            HttpPost post = new HttpPost(url);
            JSONObject response = null;
            try {
                StringEntity s = new StringEntity(json.toString(), "utf-8");
                s.setContentEncoding("utf-8");
                // 发送json数据需要设置contentType
                s.setContentType("application/json");
                post.setEntity(s);

                HttpResponse res = httpClient.execute(post);
                if (res.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    // 返回json格式：
                    String result = EntityUtils.toString(res.getEntity());
                    System.out.println(result);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            //频道id 类别id 产品id 用户id 打开时间 离开时间 地区 网络方式 来源方式 浏览器
            ClickLog clickLog = new ClickLog();

            clickLog.setChannelID(channelID[random.nextInt(channelID.length)]);
            clickLog.setCategoryID(categoryID[random.nextInt(categoryID.length)]);
            clickLog.setProduceID(produceID[random.nextInt(produceID.length)]);
            clickLog.setUserID(userID[random.nextInt(userID.length)]);
            clickLog.setCountry(contrys[random.nextInt(contrys.length)]);
            clickLog.setProvince(provinces[random.nextInt(provinces.length)]);
            clickLog.setCity(citys[random.nextInt(citys.length)]);
            clickLog.setNetwork(networks[random.nextInt(networks.length)]);
            clickLog.setSource(sources[random.nextInt(sources.length)]);
            clickLog.setBrowserType(browser[random.nextInt(browser.length)]);

            Long[] times = usetimelog.get(random.nextInt(usetimelog.size()));
            clickLog.setEntryTime(times[0]);
            clickLog.setLeaveTime(times[1]);

            String jonstr = JSONObject.toJSONString(clickLog);
            System.out.println(jonstr);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            send("http://localhost:8888/receive", jonstr);
        }
    }
}
