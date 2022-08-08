import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseDML {

    public static Connection connection = null;

    static{
        try {
            // 创建 Hbase 连接配置
            Configuration conf = HBaseConfiguration.create();
            // 设置连接 url
            conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws IOException {

        // DML 查询
        testScanData("dev","user","101","104");

        // 关闭连接
        connection.close();
    }

    /**
     * DML 查询
     * shell：
     *      get: get 'namespace:table','rk'
     *      scan: scan 'namespace:table',{STARTROW=>'startkey',STOPROW=>'stopkey'}
     */
    public static void testScanData(String namespaceName,String tableName,String startRow,String stopRow) throws IOException {
        // 调用 TableName 方法，查询指定的表
        TableName tb = TableName.valueOf(namespaceName, tableName);
        // 获取表连接
        Table table = connection.getTable(tb);
        // 新建 Scan -- 要对表进行的操作
        Scan scan = new Scan();
        // 添加限制的行
        scan.withStartRow(Bytes.toBytes(startRow)).withStopRow(Bytes.toBytes(stopRow));
        // 获取 Scan 结果
        ResultScanner scanner = table.getScanner(scan);
        // 遍历，打印扫描的结果
        for (Result result : scanner) {
            // Hbase存储最小单元
            List<Cell> cells = result.listCells();
            // 遍历输出
            for (Cell cell : cells) {
                String cl = Bytes.toString(CellUtil.cloneRow(cell)) + " : " +
                        Bytes.toString(CellUtil.cloneFamily(cell)) + " : " +
                        Bytes.toString(CellUtil.cloneQualifier(cell)) + " : " +
                        Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(cl);
            }

            System.out.println("---------------------------");
        }
        table.close();
    }
}