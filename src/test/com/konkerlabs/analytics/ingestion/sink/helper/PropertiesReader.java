package com.konkerlabs.analytics.ingestion.sink.helper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Felipe on 23/12/16.
 */
public abstract class PropertiesReader {
    public static Properties getProperties() {
        Properties properties = new Properties();
        InputStream input = null;
        try {
            input = PropertiesReader.class.getClassLoader().getResourceAsStream("test.conf");
            properties.load(input);
        } catch (IOException exception) {
            exception.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return properties;
    }
}
