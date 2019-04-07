package dtouding.logcollect;

import dtouding.common.utils.HdfsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 日志采集任务
 */
public class LogCollectTask extends TimerTask {

    private final Logger logger = LoggerFactory.getLogger(LogCollectTask.class);

    public void run() {
        /**
         * -- 检查源目录
         * -- 获取需要采集的日志
         * -- 移动日志文件到待上传目录
         * --
         */
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH");
        String date = dateFormat.format(new Date());
        File srcDir = new File(HdfsLogProperty.LOG_SRC_DIR);
        if (!srcDir.exists()) {
            logger.error("日志源目录不存在");
            return;
        }
        /*File[] listFiles = srcDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                if (name.startsWith(ConfProperties.LOG_TO_UPLOAD_PREFIX)) {
                    return true;
                }
                return false;
            }
        });*/
        /** 1. 获取需要采集的日志. */
        List<File> listFiles = Arrays.stream(srcDir.listFiles())
                .filter(e -> e.getName().startsWith(HdfsLogProperty.LOG_TO_UPLOAD_PREFIX))
                .collect(Collectors.toList());
        if (listFiles.isEmpty()) {
            logger.error("没有需要待采集的日志");
            return;
        }
        /** 2. 将日志文件移动到待上传目录. */
        File toupload = new File(HdfsLogProperty.LOG_TO_UPLOAD_DIR);
        if (!toupload.exists()) {
            toupload.mkdirs();
        }
        listFiles.stream().forEach(file ->
                file.renameTo(new File(HdfsLogProperty.LOG_TO_UPLOAD_DIR+"/"+file.getName()))
        );
        String hdfsLogDir = HdfsLogProperty.HDFS_LOG_DIR + "/" + date;
        if (!HdfsUtil.exists(hdfsLogDir)) {
            HdfsUtil.mkdirs(hdfsLogDir);
        }
        File backupDir = new File(HdfsLogProperty.LOG_BACKUP_DIR + "/" + date);
        if (!backupDir.exists()) {
            backupDir.mkdirs();
        }
        Arrays.stream(toupload.listFiles()).forEach(
                file -> {
                    /** 3. 上传日志文件到HDFS. */
                    HdfsUtil.copyFromLocalFile(file.getAbsolutePath(),
                            hdfsLogDir + "/" + HdfsLogProperty.HDFS_LOG_PREFIX
                                    + UUID.randomUUID() + HdfsLogProperty.HDFS_LOG_SUFFIX);
                    /** 4. 文件备份. */
                    file.renameTo(backupDir);
                }
        );
    }
}
