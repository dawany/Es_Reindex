package util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

/**
 * @Description:
 * @Author: ydw
 * @Date: Created in 22:32 2017/12/27
 * @Modified by () on ().
 */
public class StringUtil {

    public static String[] getIPandPort(String str) {
        String[] strArray = str.split(";");
        return strArray;
    }

    public static String[] getArrays(String str) {
        String[] strArray = str.split(";");
        return strArray;
    }

    public static String getIP(String str) {
        String ip = str.split(":")[0];
        return ip;
    }

    public static int getPort(String str) {
        String port = str.split(":")[1];
        return Integer.parseInt(port);
    }

    //获取当天日期1
    public static String getToday() {
        Date date = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        String day = dateFormat.format(date);
        return day;
    }

    //获取当天日期2
    public static String getToday2() {
        Date date = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String day = dateFormat.format(date);
        return day;
    }

    //获取昨天日期1
    public static String getYesterday() {
        Calendar c = Calendar.getInstance();
        c.add(Calendar.DATE, -1); // 目前的時間做加减
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        String day = df.format(c.getTime());
        return day;
    }

    //获取昨天日期2
    public static String getYesterday2() {
        Calendar c = Calendar.getInstance();
        c.add(Calendar.DATE, -1); // 目前的時間做加减
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String day = df.format(c.getTime());
        return day;
    }

    //获取目标日期1
    public static String getOtherDay(int num) {
        Calendar c = Calendar.getInstance();
        c.add(Calendar.DATE, num); // 目前的時間做加减
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        String day = df.format(c.getTime());
        return day;
    }

    //获取目标日期2
    public static String getOtherDay2(int num) {
        Calendar c = Calendar.getInstance();
        c.add(Calendar.DATE, num); // 目前的時間做加减
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String day = df.format(c.getTime());
        return day;
    }

    /**
     * 时间字符串格式转换，例：20170101->2017-01-01
     *
     * @param time
     * @return
     */
    public static String formalTime(String time) {
        String newTime = null;
        try {
            newTime = time.substring(0, 4) + "-" + time.substring(4, 6) + "-" + time.substring(6);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return newTime;
    }

    /**
     * 时间字符串格式转换，例：20170101->2017-1-01
     *
     * @param time
     * @return
     */
    public static String formalTime2(String time) {
        String newTime = null;
        if (Integer.parseInt(time.substring(4, 5)) == 0) {
            newTime = time.substring(0, 4) + "-" + time.substring(5, 6) + "-" + time.substring(6);
        } else {
            newTime = time.substring(0, 4) + "-" + time.substring(4, 6) + "-" + time.substring(6);
        }
        return newTime;
    }

    /**
     * 时间字符串格式转换，例：20170101->170101
     *
     * @param time
     * @return
     */
    public static String subTime(String time) {
        String newTime = null;
        try {
            newTime = time.substring(2);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return newTime;
    }

    /**
     * 将时间转换为时间戳
     *
     * @param time
     * @return
     */
    public static String dateToStamp(String inputSuffix, String time) {
        String s = formalTime(inputSuffix) + " " + time;
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = null;
        try {
            date = simpleDateFormat.parse(s);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        long ts = date.getTime();
        res = String.valueOf(ts);
        return res;
    }
    /*
         * 将时间转换为时间戳
         */
    public static String dateToStamp(String s){
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = null;
        try {
            date = simpleDateFormat.parse(s);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        long ts = date.getTime();
        res = String.valueOf(ts);
        return res;
    }
    //将时间转换成英式时间 例如："yyyy-MM-dd HH:mm:ss" ->"MMM  d"
    public static String changeToEN1(String time) {
        String timeStamp = dateToStamp(time);
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MMM d", Locale.ENGLISH);
        long lt = new Long(timeStamp);
        Date date = new Date(lt);
        res = simpleDateFormat.format(date);
        return res;
    }
    //将时间转换成英式时间 例如："yyyy-MM-dd HH:mm:ss" ->"MMM  d"
    public static String changeToEN(String time) {
        String timeStamp = dateToStamp(time);
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MMM  d", Locale.ENGLISH);
        long lt = new Long(timeStamp);
        Date date = new Date(lt);
        res = simpleDateFormat.format(date);
        return res;
    }
    //将时间转换成英式时间 例如："yyyy-MM-dd HH:mm:ss" ->"MMM  d"
    public static String changeToEN3(String time) {
        String timeStamp = dateToStamp(time);
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MMM   d", Locale.ENGLISH);
        long lt = new Long(timeStamp);
        Date date = new Date(lt);
        res = simpleDateFormat.format(date);
        return res;
    }
    //将时间转换成英式时间 例如："yyyy-MM-dd HH:mm:ss" ->"MMM  d"
    public static String changeToEN4(String time) {
        String timeStamp = dateToStamp(time);
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MMM", Locale.ENGLISH);
        long lt = new Long(timeStamp);
        Date date = new Date(lt);
        res = simpleDateFormat.format(date);
        return res;
    }

    /*
    * 将时间戳转换为时间
    */
    public static String stampToDate(String s) {
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long lt = new Long(s);
        Date date = new Date(lt);
        res = simpleDateFormat.format(date);
        return res;
    }
}
