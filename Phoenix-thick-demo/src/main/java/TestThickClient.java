import java.sql.*;

public class TestThickClient {
    public static void main(String[] args) throws SQLException {

        //1.添加连接
        String url = "jdbc:phoenix:hadoop102:2181";

        //2.获取连接
        Connection connection = DriverManager.getConnection(url);

        //3.编译 SQL语句
        PreparedStatement preparedStatement = connection.prepareStatement("select * from \"test\"");

        //4.执行语句
        ResultSet resultSet = preparedStatement.executeQuery();

        //5.输出结果
        while (resultSet.next()){
            System.out.println(resultSet.getString(1) + " : " +
                    resultSet.getString(2) + " : " + resultSet.getString(3) +
                     " : " + resultSet.getString(4) + " : " + resultSet.getString(5));
        }

        //6.关闭资源
        resultSet.close();
        preparedStatement.close();
        connection.close();
    }
}
