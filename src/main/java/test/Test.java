package test;

import util.StringUtil;
import util.TimeUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

/**
 * @Description:
 * @Author: ydw
 * @Date: Created in 23:14 2018/1/10
 * @Modified by () on ().
 */
public class Test {
    public static void main(String[] args) {
        String time = "1514830073000";
       getDayInYear(time);

        String timeString = "2018-01-02 02:07:53";
        StringUtil.changeToEN(timeString);


        //String a = "agvsaeafaf";
        //String ss = a.replaceAll("49841491","1166");
      //  System.out.println(ss);

        String  a = TimeUtil.getDayInMonth(timeString);
        String  ab = TimeUtil.getDayInWeek(timeString);
        String  abc = TimeUtil.getMonthInYear(timeString);
        //System.out.println(ab);
        System.out.println(abc);
    }



    public static String getDayInYear(String time) {
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MMM  d", Locale.ENGLISH);
        long lt = new Long(time);
        Date date = new Date(lt);
        res = simpleDateFormat.format(date);
        System.out.println(res);
        return res;
    }
}
