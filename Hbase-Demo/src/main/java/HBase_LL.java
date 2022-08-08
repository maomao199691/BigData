import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class HBase_LL {

    // 全局静态连接属性
    public static Connection connection = null;

    /** 配置连接 hbase 对象 */
    static {
        try {
            // 配置Hbase连接
            Configuration conf = HBaseConfiguration.create();

            // 添加 zookeeper url
            conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");

            // 创建连接对象
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {

            e.printStackTrace();
            throw new RuntimeException("获取hbase连接对象失败.....");
        }
    }

    public static void main(String[] args) throws IOException {

        //testScanData("dev","user","101","102");
        //testGetData("dev","order","1001");
        //testCreateNameSpace("space");


        //testScanData("space","test","001","003");
        //testDeleteData("space","test","002","f2","order");
        //testCreateTable("space","test","f1");

        testPutData("space","test","001","f2","orderID","1001");
        testPutData("space","test","002","f2","orderID","1002");
        testScanData("space","test","001","003");

        // 关闭连接对象
        connection.close();

    }

    /**
     * DML 查询
     * shell:
     *      get: get 'namespace:table','rk'
     *      scan: scan 'namespace:table',{STARTROW=>‘startRow',STOPROW=>'stopRow'}
     */
    public static void testScanData(String nameSpace,String tableName,String startRow,String stopRow) throws IOException {

        // 指定命名空间和表名
        TableName tb = TableName.valueOf(nameSpace, tableName);

        // 得到表
        Table table = connection.getTable(tb);

        // 创建 Scan 函数
        Scan scan = new Scan();

        // 扫描指定位置
        scan.withStartRow(Bytes.toBytes(startRow)).withStopRow(Bytes.toBytes(stopRow));

        // 通过表连接，获取scan结果
        ResultScanner scanner = table.getScanner(scan);

        // 遍历，对结果进行处理，得到的结果为 cell
        for (Result result : scanner) {

            // scan 返回结果为 cell 集合
            List<Cell> cells = result.listCells();

            // 遍历，对 cell 处理，cell 是 hbase 最小存储单元
            for (Cell cell : cells) {

                // cell 存储为 Hbase 的 row、family、qualifier(列)、value
                String cl = Bytes.toString(CellUtil.cloneRow(cell)) + " : " +
                        Bytes.toString(CellUtil.cloneFamily(cell)) + " : " +
                        Bytes.toString(CellUtil.cloneQualifier(cell)) + " : " +
                        Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(cl);
            }
            System.out.println("--------------------------");
        }
        table.close();
    }

    public static void testGetData(String nameSpaceName,String tableName,String rk) throws IOException {

        // 获得表连接
        TableName tb = TableName.valueOf(nameSpaceName, tableName);
        Table table = connection.getTable(tb);

        // DML -> get 方法
        Get get = new Get(Bytes.toBytes(rk));
        Result result = table.get(get);

        // get 获得的是单行结果
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            String cl = Bytes.toString(CellUtil.cloneRow(cell)) + " : " +
                    Bytes.toString(CellUtil.cloneFamily(cell)) + " : " +
                    Bytes.toString(CellUtil.cloneQualifier(cell)) + " : " +
                    Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println(cl);
        }
        System.out.println("-----------------------");
    }

    /** DML 删除
     *  shell:
     *      delete: delete 'namespace:table','rk','f1','f2' => Delete
     *      deleteall:
     *              deleteall 'namespace:table','rk','cf','cl' => DeleteColumn
     *              deleteall 'namespace:table','rk','cf' => DeleteFamily
     */
    public static void testDeleteData(String namespaceName,String tableName,String rk,String cf,String cl) throws IOException {
        // 获取表对象
        TableName tableName1 = TableName.valueOf(namespaceName, tableName);
        Table table = connection.getTable(tableName1);

        // 删除一个字段一个版本
        //Delete delete = new Delete(Bytes.toBytes(rk));
        //delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cl));
        //table.delete(delete);

        // 删除一个字段所有版本
        //Delete delete = new Delete(Bytes.toBytes(rk));
        //delete.addColumns(Bytes.toBytes(cf),Bytes.toBytes(cl));
        //table.delete(delete);

        // 删除一个列族
        Delete delete = new Delete(Bytes.toBytes(rk));
        // 指定要删除的列族
        delete.addFamily(Bytes.toBytes(cf));
        table.delete(delete);

        // 关闭表对象资源
        table.close();
    }


    /**
     * DDL 表操作
     * shell :
     *      create 'namespace:table','f1','f2'
     */
    public static void testCreateTable(String namespaceName,String tableName,String ... cfs) throws IOException {
        // 基本判空操作

        // 获取 Admin 对象 HBaseAdmin
        Admin admin = connection.getAdmin();

        // 判断表是否存在
        TableName tableName1 = TableName.valueOf(namespaceName, tableName);
        boolean exists = admin.tableExists(tableName1);
        if (exists){
            System.err.println(namespaceName + " : " + tableName + " 已经存在");
            return;
        }

        // 创建表
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName1);
        // 设置列族
        if (cfs == null || cfs.length <= 0){
            System.err.println("至少输入一个列族");
            return;
        }
        for (String cf : cfs) {
            // 创建列族描述信息
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
                    ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));
            // 创建列族
            ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
        }

        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
        admin.createTable(tableDescriptor);

        System.out.println(namespaceName + " : " + tableName + "创建成功");
        admin.close();
    }


    /**
     * DDL 增 改
     * shell: put 'namespace:table','rk','cf:cl','value'
     */
    public static void testPutData(String namespaceName,String tableName,String rk,String cf,String cl,String value) throws IOException {

        // 获取表对象
        TableName tableName1 = TableName.valueOf(namespaceName, tableName);
        Table table = connection.getTable(tableName1);

        Put put = new Put(Bytes.toBytes(rk));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cl),Bytes.toBytes(value));
        table.put(put);

        // 关闭表连接
        table.close();
    }


    /** 创建 namespace */
    public static void testCreateNameSpace(String nameSpace) throws IOException {
        // 获取管理对象
        Admin admin = connection.getAdmin();

        // 创建命名空间描述符
        NamespaceDescriptor build = NamespaceDescriptor.create(nameSpace).build();
        admin.createNamespace(build);
        // 列出所有命名空间
        NamespaceDescriptor[] ns = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor n : ns) {
            System.out.println(n.getName());
        }
    }

}
