package com.atguigu.hbase.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class WeiboUtil {

    //获取配置 conf
    private Configuration conf = HBaseConfiguration.create();;

    // 微博内容表的表名
    private static final byte[] TABLE_CONTENT = Bytes.toBytes("weibo:content");
    // 用户关系表的表名
    private static final byte[] TABLE_RELATIONS = Bytes.toBytes("weibo:relations");
    // 微博收件箱的表名
    private static final byte[] TABLE_RECEIVE_EMAIL = Bytes.toBytes("weibo:receive_content_email");

    /**
     * 初始化命名空间
     */
    public void initNameSpace() {

        HBaseAdmin admin = null;

        try {
            admin = new HBaseAdmin(conf);
            // 命名空间类型于关系数据库中的schema，可以
            NamespaceDescriptor weibo = NamespaceDescriptor.create("weibo")
                    .addConfiguration("creator", "Jinji")
                    .addConfiguration("create_time", System.currentTimeMillis() + "").build();
            admin.createNamespace(weibo);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != admin) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    /**
     *  创建微博内容表
     */
    public void createTableContent() {

        HBaseAdmin admin = null;
        try {
            admin = new HBaseAdmin(conf);

            // 创建表的描述
            HTableDescriptor content = new HTableDescriptor(TableName.valueOf(TABLE_CONTENT));
            // 创建列族描述
            HColumnDescriptor info = new HColumnDescriptor(Bytes.toBytes("info"));
            // 设置块缓存
            info.setBlockCacheEnabled(true);
            // 设置块缓存的大小
            info.setBlocksize(2097152);
            // 设置压缩方式
            info.setCompressionType(Compression.Algorithm.SNAPPY);
            // 设置版本确界
            info.setMaxVersions(1);
            info.setMinVersions(1);

            content.addFamily(info);
            admin.createTable(content);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != admin) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }





}
