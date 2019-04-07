package dtouding.logcollect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class HdfsLogProperty {

    private final static Logger logger = LoggerFactory.getLogger(HdfsLogProperty.class);

    private static Properties properties = new Properties();

    static {
        try {
            properties.load(HdfsLogProperty.class.getResourceAsStream("/hdfs_log_conf.properties"));
        } catch (IOException e) {
            logger.error("", e);
        }
    }
    private HdfsLogProperty() {

    }

    public final static String LOG_SRC_DIR = properties.getProperty("log_src_dir");
    public final static String LOG_TO_UPLOAD_DIR = properties.getProperty("log_to_upload_dir");
    public final static String LOG_BACKUP_DIR = properties.getProperty("log_backup_dir");
    public final static String LOG_TO_UPLOAD_PREFIX = properties.getProperty("log_to_upload_prefix");
    public final static String HDFS_LOG_DIR = properties.getProperty("hdfs_log_dir");
    public final static String HDFS_LOG_PREFIX = properties.getProperty("hdfs_log_prefix");
    public final static String HDFS_LOG_SUFFIX = properties.getProperty("hdfs_log_suffix");

}
