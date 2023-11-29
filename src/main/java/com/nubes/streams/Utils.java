package com.nubes.streams;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Utils {

    public static void main(String[] args) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSSXXX");
        Date date = new Date();
        System.out.println(dateFormat.format(date));
    }
}
