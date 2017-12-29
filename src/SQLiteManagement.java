
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SQLiteManagement {

    private String url;
    private Connection conn;
    private Statement statement;
    private String sql;

    /**
     * 无参的构造函数，将会以默认的url用户名和密码登录数据库
     *
     * @throws ClassNotFoundException 如果jdbc未能加载则可能会抛出异常
     * @throws SQLException 如果没有相应的数据库和表可能在新建时抛出异常
     */
    SQLiteManagement(int Id) throws ClassNotFoundException, SQLException {
        File Users = new File("Users/" + Id);
        if (!Users.exists() || !Users.isDirectory()) {
            Users.mkdirs();
        }
        url = "jdbc:sqlite:./Users/" + Id + "/database.db";
        init();
    }

    /**
     * 有参数的构造函数，将会用参数初始化成员变量
     *
     * @param url 数据库的url
     * @throws ClassNotFoundException 如果jdbc未能加载则可能会抛出异常
     * @throws SQLException 如果没有相应的数据库和表可能在新建时抛出异常
     */
    SQLiteManagement(String url) throws ClassNotFoundException, SQLException {
        this.url = url;
        init();
    }

    /**
     * 初始化jdbc驱动并且检测是否有相应的数据库（只有在没有数据库的情况下才会新建数据库和表）
     *
     * @throws ClassNotFoundException 如果jdbc未能加载则可能会抛出异常
     * @throws SQLException 如果没有相应的数据库和表可能在新建时抛出异常
     */
    private void init() throws ClassNotFoundException, SQLException {
        Class.forName("org.sqlite.JDBC");
        connect();
        checkTable();
    }

    private void checkTable() throws SQLException {
        sql = "SELECT * FROM UserInfo";
        try {
            querySql();
        } catch (SQLException e) {
            createUserInfoTable();
        }
        sql = "SELECT * FROM ChatMessage";
        try {
            querySql();
        } catch (SQLException e) {
            createChatMessageTable();
        }
    }

    /**
     * 连接数据库并初始化conn和statement
     *
     * @throws SQLException 如果没有相应的数据库和表可能抛出异常
     */
    private void connect() throws SQLException {
        conn = DriverManager.getConnection(url);
        statement = conn.createStatement();
    }

    /**
     * 如果传入的实例非空且未关闭，则关闭
     *
     * @param o 传入的实例
     * @throws SQLException 关闭的过程中可能抛出异常
     */
    private void close(Object o) throws SQLException {
        if (o != null) {
            if (o instanceof Connection && !((Connection) o).isClosed()) {
                ((Connection) o).close();
            } else if (o instanceof Statement && !((Statement) o).isClosed()) {
                ((Statement) o).close();
            } else if (o instanceof ResultSet && !((ResultSet) o).isClosed()) {
                ((ResultSet) o).close();
            }
        }
    }

    /**
     * 将ResultSet，Statement，Connection都传进来并且一起关闭
     *
     * @param conn 要关闭的conn
     * @param statement 要关闭的statement
     * @param rs 要关闭的rs
     * @throws SQLException 关闭的过程中可能抛出异常
     */
    private void close(Connection conn, Statement statement, ResultSet rs) throws SQLException {
        rs.close();
        statement.close();
        conn.close();
    }

    /**
     * 新建一个UserInfo表
     *
     * @throws SQLException 新建表的时候可能因为sql语句而抛出异常
     */
    private void createUserInfoTable() throws SQLException {
        sql = "CREATE TABLE UserInfo\n" +
                "(\n" +
                "    Id INTEGER PRIMARY KEY NOT NULL,\n" +
                "    Username TEXT NOT NULL\n" +
                ");";
        updateSql();
    }

    /**
     * 新建一个ChatMessage表
     *
     * @throws SQLException 新建表的时候可能因为sql语句而抛出异常
     */
    private void createChatMessageTable() throws SQLException {
        sql = "CREATE TABLE ChatMessage\n" +
                "(\n" +
                "    Id INTEGER PRIMARY KEY NOT NULL,\n" +
                "    Date TEXT NOT NULL,\n" +
                "    \"From\" INTEGER NOT NULL,\n" +
                "    \"To\" INTEGER NOT NULL,\n" +
                "    MessageType TEXT NOT NULL,\n" +
                "    Message TEXT NOT NULL,\n" +
                "    SubMessage TEXT\n" +
                ");";
        updateSql();
    }

    /**
     * 设定将要执行的sql语句
     *
     * @param sql sql语句
     */
    void setSql(String sql) {
        this.sql = sql;
    }

    /**
     * 对数据库进行查找并且将返回值通过Map保存在一个List中
     *
     * @return 返回查询的结果
     * @throws SQLException 在执行sql语句的时候可能抛出异常
     */
    List querySql() throws SQLException {
        if (sql == null) {
            System.out.println("empty sql");
            return null;
        }
        List resultList = new ArrayList();
        connect();
        ResultSet rs = statement.executeQuery(sql);
        ResultSetMetaData rsMetaData = rs.getMetaData();
        while (rs.next()) {
            Map<String, String> result = new HashMap<String, String>();
            for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
                result.put(rsMetaData.getColumnName(i),rs.getString(i));
            }
            resultList.add(result);
        }
        close(conn,statement,rs);
        sql = null;
        return resultList;
    }

    /**
     * 对数据库的内容进行变更和添加并且返回新增的Id
     *
     * @return 返回新增数据行的Id
     * @throws SQLException 在执行sql语句的时候可能抛出异常
     */
    int updateSql () throws SQLException {
        //执行sql语句
        if (sql == null) {
            System.out.println("empty sql");
            return -1;
        }
        connect();
        statement.executeUpdate(sql);
        //获取新增的数据行的Id
        ResultSet result = statement.getGeneratedKeys();
        int num = -1;
        if (result.next()) {
            num = result.getInt(1);
        }
        close(statement);
        close(conn);
        sql = null;
        return num;
    }

    /**
     * 更新本地数据库，将服务器返回的数据保存到本地数据库中
     *
     * @param m 远端服务器发送过来的数据
     * @throws SQLException 更新数据库时可能抛出异常
     */
    void updateChatMessage(Map m) throws Exception {
        List l = (List) m.get("Message");
        for (int i = 0; i < l.size(); i++) {
            Map tmp = (Map) l.get(i);
            sql = "SELECT * FROM ChatMessage WHERE Id = " + tmp.get("Id");
            if (querySql().isEmpty()) {
                sql = "INSERT INTO ChatMessage VALUES (" +
                        tmp.get("Id") + ",\"" +
                        tmp.get("Date") + "\"," +
                        tmp.get("From") + "," +
                        tmp.get("To") + ",\"" +
                        tmp.get("MessageType") + "\",\"" +
                        tmp.get("Message") + "\",\"" +
                        tmp.get("SubMessage") + "\"" +
                        ")";
                updateSql();
            }
        }
        updateUserInfo((List) m.get("Friends"));
    }

    /**
     * 更新本地数据库，从聊天消息表中获取到好友信息
     *
     * @throws SQLException 更新数据库的时候可能抛出异常
     */
    void updateUserInfo(List l) throws Exception {
        for (int i = 0; i < l.size(); i++) {
            Map m = (Map) l.get(i);
            sql = "SELECT * FROM UserInfo WHERE Id = " + m.get("Id");
            List queryList = querySql();
            if (!queryList.isEmpty()) {
                Map tmp = (Map) queryList.get(0);
                if (m.get("Username") != tmp.get("Username")) {
                    sql = "UPDATE UserInfo SET Username = \"" + m.get("Username") + "\" WHERE Id = " + m.get("From");
                }
                else
                    continue;
            } else
                sql = "INSERT INTO UserInfo VALUES (" +
                        m.get("From") + ",\"" +
                        m.get("Username") + "\"" +
                        ")";
            updateSql();
        }
    }

    /**
     * 查询用户的信息
     * @return 返回用户的信息的List<Map>，键为"Id""Username"
     * @throws SQLException 访问数据库的时候可能抛出异常
     */
    List queryFriendList() throws SQLException {
        sql = "SELECT * FROM UserInfo";
        List l = querySql();
        return l;
    }

    /**
     * 从本地数据库中取出与对应用户的聊天记录
     *
     * @param from 指定用户的Id
     * @return 返回一个List，保存着聊天信息
     * @throws SQLException 访问数据库的时候可能抛出异常
     */
    List queryChatMessage(int from) throws SQLException {
        sql = "SELECT * FROM ChatMessage WHERE `From` = " + from;
        return querySql();
    }
}
