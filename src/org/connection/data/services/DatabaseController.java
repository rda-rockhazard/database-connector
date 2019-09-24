package org.connection.data.services;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.connection.data.connectivity.OdbcConnection;

/**
 * This class is meant to perform operations on the database. It uses the
 * OdbcConection class to connect with the database.
 *
 * @see OdbcConnection
 * @author RocKHaZarD
 */
public class DatabaseController {

    /**
     * Initializes connection with the database with the given parameters.
     *
     * @param host Hostname/IpAddress.
     * @param portNumber Port no on which your MySQL is configured.
     * @param databaseName Name of database you want to create or already
     * created.
     * @param databaseUsername Username of MySQL
     * @param databasePassword Password for this username.
     */
    public DatabaseController(String host, String portNumber, String databaseName, String databaseUsername, String databasePassword) {
        OdbcConnection.getConnection(host, portNumber, databaseName, databaseUsername, databasePassword);
    }

    /**
     * This method is capable of executing all the database related queries like
     * INSERT, UPDATE, DELETE, ALTER, CREATE, DROP, CALL, etc except the SELECT
     * query.
     *
     * @param query The query in StringBuilder format to be executed.
     * @return Returns whether the query is executed or not.
     */
    public boolean execute(StringBuilder query) {
        try {
            OdbcConnection.st.execute(query.toString());
            return true;
        } catch (SQLException se) {
            Logger.getLogger(DatabaseController.class.getName()).log(Level.SEVERE, null, se);
        }
        return false;
    }

    /**
     * This method is created for the execution of SELECT query.
     *
     * @param query The SELECT query in StringBuilder format to be executed.
     * @return Returns the Map of row index and List of data in that row.
     */
    public Map<Integer, List<String>> executeQuery(StringBuilder query) {
        Map<Integer, List<String>> resultSet = new LinkedHashMap<>();
        try {
            OdbcConnection.rs = OdbcConnection.st.executeQuery(query.toString());
            ResultSetMetaData metaData = OdbcConnection.rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            int rowIter = 1;
            while (OdbcConnection.rs.next()) {
                List<String> dataList = new LinkedList<String>();
                for (int i = 1; i <= columnCount; i++) {
                    dataList.add(OdbcConnection.rs.getString(i));
                }
                resultSet.put(rowIter, dataList);
                rowIter++;
            }
            OdbcConnection.rs.close();
        } catch (SQLException se) {
            Logger.getLogger(DatabaseController.class.getName()).log(Level.SEVERE, null, se);
        }
        return resultSet;
    }

    /**
     * Closes the connection established by OdbcConnection.
     */
    public void closeConnection() {
        try {
            OdbcConnection.getCon().close();
        } catch (SQLException se) {
            Logger.getLogger(DatabaseController.class.getName()).log(Level.SEVERE, null, se);
        }
    }

}
