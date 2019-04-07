package dtouding.loggernatetor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;

public class LogGenerateTask extends TimerTask {

    private final Logger logger = LoggerFactory.getLogger(LogGenerateTask.class);
    public void run() {
        logger.info("和羞走，倚门回首，却把青梅嗅!");
        logger.info("大江东去浪淘尽，千古风流任务!");
        logger.info("人生若只如初见，何事秋风悲画扇。");
        logger.info("竹杖芒鞋轻胜马，谁怕？一蓑烟雨任平生!");
    }
}
