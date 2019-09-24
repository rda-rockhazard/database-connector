package org.connection.data.connectivity;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class is the meant for the database connectivity purpose over the user
 * defined configuration parameters. Using its connection one can do database
 * operations.
 *
 * @author RocKHaZarD
 */
public class OdbcConnection {

    static private Connection con;
    static public Statement st;
    static public ResultSet rs;
    static private String connectionString;

    /**
     * This method initializes connection over configuration parameters. If the
     * provided database does not exist then it creates a new one with the
     * provided name in the configuration.
     *
     * @param config Configuration parameters <br>
     *
     * 1 - Hostname or IpAddress, 2 - Port number, 3 - Database name, 4 -
     * Username and 5 - Password
     */
    public static void getConnection(String... config) {
        try {
            connectionString = "jdbc:mysql://" + config[0] + ":" + config[1];
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection(connectionString, config[3], config[4]);
            st = con.createStatement();
            createDataBaseIfNotExist(config);
            con.setAutoCommit(true);
            System.out.println("Connection Established " + con + " on Statement " + st);
        } catch (SQLException | ClassNotFoundException e) {
            Logger.getLogger(OdbcConnection.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    /**
     * Creates the new database with the name provided if it is not already
     * created and reconnects over direct database stream for user convenience.
     *
     * @param config Configuration parameters.
     */
    private static void createDataBaseIfNotExist(String... config) {
        try {
            st.execute("CREATE DATABASE IF NOT EXISTS `" + config[2] + "`;");
            connectionString = "jdbc:mysql://" + config[0] + ":" + config[1] + "/" + config[2];
            con = DriverManager.getConnection(connectionString, config[3], config[4]);
            st = con.createStatement();
        } catch (SQLException ex) {
            Logger.getLogger(OdbcConnection.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * It returns the Connection configured over the provided parameters.
     *
     * @see Connection
     * @return Connection
     */
    public static Connection getCon() {
        return con;
    }
}
