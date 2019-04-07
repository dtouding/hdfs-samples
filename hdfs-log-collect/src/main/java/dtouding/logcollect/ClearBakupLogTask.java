package dtouding.logcollect;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.TimerTask;

/**
 * 删除备份日志文件
 */
public class ClearBakupLogTask extends TimerTask {

    @Override
    public void run() {
        File backupDir = new File(HdfsLogProperty.LOG_BACKUP_DIR);
        long now = Instant.now().getEpochSecond();
        if (backupDir.exists()) {
            Arrays.stream(backupDir.listFiles()).forEach(
                    file -> {
                        long start = LocalDateTime.parse(file.getName(),
                                DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"))
                                .toEpochSecond(ZoneOffset.of("+8"));
                        if (now-start > 24*60*60L) {
                            try {
                                FileUtils.deleteDirectory(file);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
            );
        }
    }
}
