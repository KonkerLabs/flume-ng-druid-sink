package com.konkerlabs.analytics.ingestion.sink.helper;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Created by Felipe on 23/12/16.
 */
public abstract class DateFormater {
    public static DateTimeFormatter getDateFormatter(String timestampFormat) {
        DateTimeFormatter dateTimeFormatter;
        if (timestampFormat.equals("auto")) {
            dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
        } else if (timestampFormat.equals("millis")) {
            dateTimeFormatter = null;
        } else {
            dateTimeFormatter = DateTimeFormat.forPattern(timestampFormat);
        }
        return dateTimeFormatter;
    }
}
