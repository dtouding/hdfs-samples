package dtouding.loggernatetor;

import java.util.Timer;

/**
 * 生成日志入口，
 * 供hdfs日志采集使用
 */
public class LogGenerateMain {

    public static void main(String[] args) {
        Timer timer = new Timer();
        timer.schedule(new LogGenerateTask(), 0, 1000L);
    }
}
