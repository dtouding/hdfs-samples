package dtouding.logcollect;

import java.util.Timer;

/**
 * 日志采集入口
 */
public class LogCollectMain {

    public static void main(String[] args) {
        Timer timer = new Timer();
        /** 日志采集任务. */
        timer.schedule(new LogCollectTask(), 0, 60*60*1000L);
        /** 删除备份日志目录. */
        timer.schedule(new ClearBakupLogTask(), 0, 60*60*1000L);
    }
}
