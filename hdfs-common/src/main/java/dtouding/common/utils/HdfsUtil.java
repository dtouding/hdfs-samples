package dtouding.common.utils;

import dtouding.common.config.HdfsProperty;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

/**
 * HDFS工具类
 */
public class HdfsUtil {

    private static final Logger logger = LoggerFactory.getLogger(HdfsUtil.class);

    /**
     * 获取访问HDFS文件系统的客户端对象
     *
     * @return
     */
    public static FileSystem getFs() {
        /**
         * Configuration参数对象加载机制：
         * 构造时，会加载jar包中的默认配置xx-default.xml
         * 在加载用户配置的xx-site.xml
         * 构造完成后，还可以conf.set("", "")，会再次覆盖用户配置的参数
         */
        Configuration conf = new Configuration();
        /** 指定客户单上传文件到hdfs时需要保存的副本数为：2*/
        conf.set("dfs.replication", "2");
        /** 指定客户端上传文件到hdfs时切块的大小：64M*/
        conf.set("dfs.blocksize", "64m");
        /**
         * 构造一个访问HDFS系统个客户端对象
         *  参数1：HDFS系统的URI
         *  参数2：客户端特别指定的参数
         *  参数3：客户端的身份
         **/
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI(HdfsProperty.HDFS_URL), conf, HdfsProperty.HDFS_USER);
        } catch (Exception e) {
            logger.error("", e);
        }
        return fs;
    }

    /**
     * 上传一个文件到HDFS
     * @param src
     * @param dest
     */
    public static void copyFromLocalFile(String src, String dest) {
        FileSystem fs = getFs();
        try {
            if (null != fs) {
                fs.copyFromLocalFile(new Path(src), new Path(dest));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeFs(fs);
        }
    }

    /**
     * 将HDFS中的文件下载到客户端
     * @param src
     * @param dest
     */
    public static void copyToLocalFile(String src, String dest) {
        FileSystem fs = getFs();
        if (null != fs) {
            try {
                fs.copyToLocalFile(new Path(src), new Path(dest));
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                closeFs(fs);
            }
        }
    }

    /**
     * 在HDfs系统内，将路径src改为dest
     * 类似linux中的mv命令
     * @param src
     * @param dest
     */
    public static void rename(String src, String dest) {
        FileSystem fs = getFs();
        if (null != fs) {
            try {
                fs.rename(new Path(src), new Path(dest));
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                closeFs(fs);
            }
        }
    }

    /**
     * 在HDFS中创建目录
     * 类似linux mkdir -p 命令
     * @param dir
     */
    public static void mkdirs(String dir) {
        FileSystem fs = getFs();
        if (null != fs) {
            try {
                fs.mkdirs(new Path(dir));
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                closeFs(fs);
            }
        }
    }

    /**
     * 删除HDFS中的文件目录，flag表示是否级联删除
     * @param dir
     * @param flag
     */
    public static void delete(String dir, boolean flag) {
        FileSystem fs = getFs();
        if (null != fs) {
            try {
                fs.delete(new Path(dir), flag);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                closeFs(fs);
            }
        }
    }

    /**
     * 查询HDFS指定路径下的文件
     *
     * @param path
     */
    public static void listFiles(String path, boolean recursive) {
        FileSystem fs = getFs();
        if (null != fs) {
            try {
                RemoteIterator<LocatedFileStatus> ri = fs.listFiles(new Path(path), recursive);
                while (ri.hasNext()) {
                    LocatedFileStatus fileStatus = ri.next();
                    System.out.println("文件全路径：" + fileStatus.getPath());
                    System.out.println("块大小：" + fileStatus.getBlockSize());
                    System.out.println("文件长度：" + fileStatus.getLen());
                    System.out.println("副本数量：" + fileStatus.getReplication());
                    System.out.println("块信息：" + Arrays.toString(fileStatus.getBlockLocations()));
                }
            } catch (IOException e) {
                logger.error("", e);
            } finally {
                closeFs(fs);
            }
        }
    }

    public static boolean exists(String s) {
        FileSystem fs = getFs();
        if (null != fs) {
            try {
                return fs.exists(new Path(s));
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                closeFs(fs);
            }
        }
        throw new RuntimeException("HDFS error.");
    }
    /**
     * 关闭HDFS对象
     * @param fs
     */
    public static void closeFs(FileSystem fs) {
        if (null != fs) {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
