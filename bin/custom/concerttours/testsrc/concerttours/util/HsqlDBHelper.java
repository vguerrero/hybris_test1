package concerttours.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;


/**
 * A helper to allow users to directly invoke HSQL queries from hybris123
 */
@SuppressWarnings({"WeakerAccess", "SqlNoDataSourceInspection", "unused"})
public class HsqlDBHelper {
    private static final String HSQLDB = "jdbc:hsqldb:file:C:\\hybris_test\\CXCOMM190500P_13-70004140\\hybris\\data\\hsqldb\\mydb;hsqldb.tx=mvcc;shutdown=true;hsqldb.log_size=8192;hsqldb.large_data=true";
    private static final Logger LOG = LoggerFactory.getLogger(HsqlDBHelper.class);
    private Connection conn;

    public HsqlDBHelper() throws ClassNotFoundException, SQLException {
        Class.forName("org.hsqldb.jdbcDriver");        // Loads the HSQL Database Engine JDBC driver
        // !!Note that leaving your default password as the empty string in production would be a major security risk!!
        conn = DriverManager.getConnection(HSQLDB, "sa", "");
    }

    public void shutdown() throws SQLException {
        Statement st = conn.createStatement();
        st.execute("SHUTDOWN");
        conn.close();
    }

    public synchronized String select(String expression) throws SQLException {
        Statement st;
        ResultSet rs;
        st = conn.createStatement();
        rs = st.executeQuery(expression);
        String res = dump(rs);
        st.close();
        return res;
    }

    public String dump(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int colmax = meta.getColumnCount();
        int i;
        String o;
        String result = "";
        while (rs.next()) {
            for (i = 1; i <= colmax; i++) {
                if (i > 1)
                    result = result.concat(" ");
                o = (rs.getObject(i) == null) ? "NULL" : rs.getObject(i).toString();
                result = result.concat(o);
            }
            result = result.concat("\n");
        }
        return result;
    }
}


