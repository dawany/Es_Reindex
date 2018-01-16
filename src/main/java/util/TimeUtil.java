package util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @Description:
 * @Author: ydw
 * @Date: Created in 21:02 2018/1/10
 * @Modified by () on ().
 */
public class TimeUtil {

    //时间戳格式：yyyy-MM-dd HH:mm:ss

    public static Date timeStamptoDate(String time) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String d = format.format(time);
        Date date = null;
        try {
            date = format.parse(d);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }


    //时间戳转换成字符串，提取小时;
    public static String getHour(String time) {
        Long timeLong = null;
        try {
            timeLong = Long.parseLong(time);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
        SimpleDateFormat sdf = new SimpleDateFormat("HH");
        return sdf.format(timeLong);
    }

    //时间戳转换成字符串，提取分;
    public static String getMinute(String time) {
        Long timeLong = null;
        try {
            timeLong = Long.parseLong(time);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
        SimpleDateFormat sdf = new SimpleDateFormat("mm");
        return sdf.format(timeLong);
    }

    //时间戳转换成字符串，提取秒;
    public static String getSecond(String time) {
        Long timeLong = null;
        try {
            timeLong = Long.parseLong(time);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
        SimpleDateFormat sdf = new SimpleDateFormat("ss");
        return sdf.format(timeLong);
    }

    //时间戳转换成字符串，提取年;
    public static String getYear(String time) {
        Long timeLong = null;
        try {
            timeLong = Long.parseLong(time);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy");
        return sdf.format(timeLong);
    }

    //时间戳转换成字符串，提取月;
    public static String getMonth(String time) {
        Long timeLong = null;
        try {
            timeLong = Long.parseLong(time);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
        SimpleDateFormat sdf = new SimpleDateFormat("MM");
        return sdf.format(timeLong);
    }

    //时间戳转换成字符串，提取日;
    public static String getDay(String time) {
        Long timeLong = null;
        try {
            timeLong = Long.parseLong(time);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
        SimpleDateFormat sdf = new SimpleDateFormat("dd");
        return sdf.format(timeLong);
    }


    //时间戳转换成字符串，获取一年的第几天;
    public static String getDayInYear(String time) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date date = null;
        try {
            date = sdf.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar ca = Calendar.getInstance();
        ca.setTime(date);
        return String.valueOf(ca.get(Calendar.DAY_OF_YEAR));
    }

    //时间戳转换成字符串，获取每月的第几天;
    public static String getDayInMonth(String time) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date date = null;
        try {
            date = sdf.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar ca = Calendar.getInstance();
        ca.setTime(date);
        return String.valueOf(ca.get(Calendar.DAY_OF_MONTH));
    }

    //时间戳转换成字符串，获取一年的第几周;
    public static String getWeekInYear(String time) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date date = null;
        try {
            date = sdf.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar ca = Calendar.getInstance();
        ca.setTime(date);
        return String.valueOf(ca.get(Calendar.WEEK_OF_YEAR));
    }

    //时间戳转换成字符串，获取每月的第几周;
    public static String getWeekInMonth(String time) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date date = null;
        try {
            date = sdf.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar ca = Calendar.getInstance();
        ca.setTime(date);
        return String.valueOf(ca.get(Calendar.WEEK_OF_MONTH));
    }

    //时间戳转换成字符串，获取每周第几天;
    public static String getDayInWeek(String time) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date date = null;
        try {
            date = sdf.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar ca = Calendar.getInstance();
        ca.setTime(date);
        return String.valueOf(ca.get(Calendar.DAY_OF_WEEK));
    }

    //时间戳转换成字符串，获取一年的第几月;
    public static String getMonthInYear(String time) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date date = null;
        try {
            date = sdf.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar ca = Calendar.getInstance();
        ca.setTime(date);
        return String.valueOf(ca.get(Calendar.MONTH));
    }

    //获取当前的系统时间
    public static String getSystemTime(){
        Date time=new Date();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(time);
    }

}
