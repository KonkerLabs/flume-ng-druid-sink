package com.konkerlabs.analytics.ingestion.sink.exception;

/**
 * Created by Felipe on 23/12/16.
 */
public class FlumeEventParserException extends Exception {
    public FlumeEventParserException(String message) {
        super(message);
    }
}
