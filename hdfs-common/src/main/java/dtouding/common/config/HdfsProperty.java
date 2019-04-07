package dtouding.common.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class HdfsProperty {

    private final static Logger logger = LoggerFactory.getLogger(HdfsProperty.class);

    private static Properties properties = new Properties();

    static {
        try {
            properties.load(HdfsProperty.class.getResourceAsStream("/conf.properties"));
        } catch (IOException e) {
            logger.error("", e);
        }
    }
    private HdfsProperty() {

    }

    public final static String HDFS_URL = properties.getProperty("hdfs_url");
    public final static String HDFS_USER = properties.getProperty("hdfs_user");
}
