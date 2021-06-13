package com.atguigu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseUtil {

    public static Configuration conf;

    static {
        // 使用HBaseConfiguration的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.10.130");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    /**
     * 创建连接
     * @return
     */
    public static HBaseAdmin createAdmin() throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        return admin;
    }

    /**
     * 判断表是否存在
     * @param tableName
     * @return
     */
    public static boolean isTableExist(HBaseAdmin admin, String tableName) throws IOException {
        // 在HBase中管理、访问表需要先创建HBaseAdmin对象
        // Connection
        return admin.tableExists(tableName);
    }

    /**
     * 创建表
     * @param tableName
     * @param columnFamily
     */
    public static boolean createTable(String tableName, String... columnFamily) throws IOException {
        HBaseAdmin admin = createAdmin();
        if (isTableExist(admin, tableName)) {  // 判断表是否存在
            System.out.println("表" + tableName + "已经存在");
            return false;
        } else {
            // 创建表
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            // 创建多个列族
            for (String cf : columnFamily) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }

            // 根据对表的配置，创建表
            admin.createTable(descriptor);
            System.out.println("表" + tableName + "创建成功!");
            return true;
        }
    }

    /**
     * 删除表
     * @param tableName
     * @return
     */
    public static boolean dropTable(String tableName) throws IOException {

        HBaseAdmin admin = createAdmin();

        if (isTableExist(admin, tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("表" + tableName + "删除成功！");
            return true;
        }

        System.out.println("表" + tableName + "不存在");
        return false;
    }

    /**
     * 向表中插入数据
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param value
     * @return
     */
    public static void addRowData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {

        // 创建HTable对象
        HTable hTable = new HTable(conf, tableName);
        // 向表中插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        // 向Put对象中组装数据
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        hTable.put(put);
        hTable.close();
        System.out.println("插入数据成功");
    }

    /**
     * 删除多行数据
     * @param tableName
     * @param rows
     * @throws IOException
     */
    public static void deleteMultiRow(String tableName, String... rows) throws IOException {
        HTable table = new HTable(conf, tableName);
        List<Delete> deleteList = new ArrayList<>();
        for (String row : rows) {
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        table.delete(deleteList);
        table.close();
    }

    /**
     *
     * @param tableName
     * @throws IOException
     */
    public static void getAllRows(String tableName) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        // 得到用于扫描region的对象
        Scan scan = new Scan();
        // 使用HTable得到resultScanner实现类的对象
        ResultScanner resultScanner = hTable.getScanner(scan);
        for (Result result : resultScanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : result.rawCells()) {
                // 得到rowKey
                System.out.println("行键：" + Bytes.toString(CellUtil.cloneRow(cell)));
                // 得到列族
                System.out.println("列族：" + Bytes.toString(CellUtil.cloneFamily(cell)));
                // 列
                System.out.println("列：" + CellUtil.cloneQualifier(cell));
                // 值
                System.out.println("值：" + CellUtil.cloneValue(cell));
            }
        }
    }

    /**
     * 获取一行
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void getRow(String tableName, String rowKey) throws IOException {

        HTable hTable = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        //get.setMaxVersions();显示所有版本
        //get.setTimeStamp();显示指定时间戳的版本
        Result result = hTable.get(get);
        for (Cell cell : result.rawCells()) {
            // 得到rowKey
            System.out.println("行键：" + Bytes.toString(CellUtil.cloneRow(cell)));
            // 得到列族
            System.out.println("列族：" + Bytes.toString(CellUtil.cloneFamily(cell)));
            // 列
            System.out.println("列：" + CellUtil.cloneQualifier(cell));
            // 值
            System.out.println("值：" + CellUtil.cloneValue(cell));

            // 时间戳
            System.out.println("时间戳：" + cell.getTimestamp());
        }

    }

    /**
     * 获取某一行指定“列族:列”的数据
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @throws IOException
     */
    public static void getRowQualifier(String tableName, String rowKey, String family, String qualifier) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));

        get.addColumn(Bytes.toBytes(family),
                Bytes.toBytes(qualifier));

        Result result = hTable.get(get);
        for(Cell cell : result.rawCells()){
            System.out.println(" 行 键 :" +
                    Bytes.toString(result.getRow()));
            System.out.println(" 列 族 " +
                    Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println(" 列 :" +
                    Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println(" 值 :" +
                    Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

}
