package dtouding.wordcount;

import dtouding.common.utils.HdfsUtil;
import org.apache.hadoop.fs.*;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * hdfs word count
 */
public class WordCountMain {

    public static void main(String[] args) throws Exception {
        FileSystem fs = HdfsUtil.getFs();
        if (fs == null) {
            return;
        }
        Map<String, Integer> wordCountMap = new HashMap<>();
        RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path("/hdfs_test/logs/2019-04-07-17"), false);
        while (iter.hasNext()) {
            LocatedFileStatus fileStatus = iter.next();
            FSDataInputStream fis = fs.open(fileStatus.getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
                String[] words = line.split("-");
                for (String word : words) {
                    if (wordCountMap.containsKey(word)) {
                        Integer count = wordCountMap.get(word);
                        count++;
                        wordCountMap.put(word, count);
                    } else {
                        wordCountMap.put(word, 1);
                    }
                }
            }
        }
        System.out.println(wordCountMap);
    }
}
