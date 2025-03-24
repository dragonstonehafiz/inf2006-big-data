package Utility;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.ZonedDateTime;

public class TimestampConverter {
    
    public static String convert_yy_mm(long timestamp)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM");
        ZonedDateTime dateTime = Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault());
        String formatted = dateTime.format(formatter);
        return formatted;
    }
    
    public static String convert_yy(long timestamp)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy");
        ZonedDateTime dateTime = Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault());
        String formatted = dateTime.format(formatter);
        return formatted;
    }
}

