package github.abendt.highlander.jdbc;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.rules.ExternalResource;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;

class HighlanderInMemoryDbRule extends ExternalResource {

    private String dbUrl = "jdbc:h2:mem:highlander";

    private String sql = "CREATE TABLE HIGHLANDER " +
            "(GROUPNAME     VARCHAR(255) not NULL, " +
            " ID            VARCHAR(255), " +
            " HEARTBEAT     TIMESTAMP, " +
            " VERSION       INTEGER not NULL, " +
            " PRIMARY KEY ( GROUPNAME ))";

    private Connection primaryConnection;

    private List<Connection> connections = new ArrayList();

    public DataSource getDataSource() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL(dbUrl);
        dataSource.setUser("sa");
        dataSource.setPassword("");

        return dataSource;
    }

    public Connection getConnection() {
        Connection conn = createConnection();

        connections.add(conn);

        return conn;
    }

    @Override
    protected void before() throws Throwable {
        primaryConnection = createConnection();
        Statement stmt = primaryConnection.createStatement();
        stmt.executeUpdate(sql);
    }

    @Override
    protected void after() {
        try {
            for (Connection c : connections) {
                try {
                    c.close();
                } catch (Exception e) {
                }
            }

            connections.clear();

            Statement stmt = primaryConnection.createStatement();
            stmt.executeUpdate("DROP TABLE HIGHLANDER");
            primaryConnection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void insertElection(String groupName, String id, java.util.Date heartBeat) throws SQLException {

        StringBuilder columns = new StringBuilder("(GROUPNAME, ");
        StringBuilder values = new StringBuilder("(?, ");
        if (id != null) {
            columns.append("ID, ");
            values.append("?, ");
        }

        if (heartBeat != null) {
            columns.append("HEARTBEAT, ");
            values.append("?, ");
        }

        columns.append("VERSION)");
        values.append("?)");

        PreparedStatement st = primaryConnection.prepareStatement("INSERT INTO HIGHLANDER " + columns.toString() + " VALUES " + values.toString());

        int index = 1;
        st.setString(index++, groupName);

        if (id != null) {
            st.setString(index++, id);
        }

        if (heartBeat != null) {
            st.setTimestamp(index++, new Timestamp(heartBeat.getTime()));
        }
        st.setInt(index++, 0);

        st.execute();
        st.close();
    }

    public Date getHeartBean(String groupName) throws SQLException {
        ResultSet rs = primaryConnection.createStatement().executeQuery("SELECT HEARTBEAT FROM HIGHLANDER WHERE GROUPNAME = '" + groupName + "'");

        rs.next();
        return rs.getTimestamp("HEARTBEAT");
    }

    private Connection createConnection() {
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            throw new AssertionError();
        }

        try {
            return DriverManager.getConnection(dbUrl, "sa", "");
        } catch (SQLException e) {
            throw new RuntimeException();
        }
    }
}
