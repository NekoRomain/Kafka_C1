package tp.database;


import java.sql.*;
import java.time.LocalDateTime;

public class Database {

    private static final Database instance = new Database();

    private Connection connection = null;

    private Database (){
        connection = connect();
    }

    /*PARAMETTRE DE CONNEXION A LA BASE DE DONNEES POSTGRE*/
    private final String url = "jdbc:postgresql://127.0.0.1:5432/tpkafka";
    private final String user = "postgres";
    private final String password = "test";


//    private final String url = "jdbc:postgresql://kandula.db.elephantsql.com:5432/pornlkar";
//    private final String user = "pornlkar";
//    private final String password = "mbdpf5meee5IsCGJXyf80q3Gj9-L0lOz";



    //

    private final String CREATE_TABLE = "DROP TABLE IF EXISTS global; " +
            "DROP TABLE IF EXISTS countries; " +
            "CREATE TABLE global(" +
            "    id INT PRIMARY KEY NOT NULL," +
            "    new_confirmed bigint," +
            "    total_confirmed bigint," +
            "    new_deaths bigint, " +
            "    total_deaths bigint," +
            "    new_recovered bigint," +
            "    total_recovered bigint," +
            "    date_maj TIMESTAMP" +
            "); " +
            "CREATE TABLE countries(" +
            "    country VARCHAR(200) PRIMARY KEY," +
            "    country_code VARCHAR(6)," +
            "    slug VARCHAR(200)," +
            "    new_confirmed bigint," +
            "    total_confirmed bigint," +
            "    new_deaths bigint," +
            "    total_deaths bigint," +
            "    new_recovered bigint," +
            "    total_recovered bigint," +
            "    date_maj TIMESTAMP" +
            ");";


    private final String INSERT_UPDATE_SQL = "INSERT INTO global (id, new_confirmed, total_confirmed, new_deaths, total_deaths, new_recovered, total_recovered, date_maj) " +
            "VALUES " +
            "(?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (id) DO UPDATE " +
            "SET new_confirmed = EXCLUDED.new_confirmed, " +
            "total_confirmed = EXCLUDED.total_confirmed," +
            "new_deaths = EXCLUDED.new_deaths, " +
            "total_deaths = EXCLUDED.total_deaths," +
            "new_recovered = EXCLUDED.new_recovered," +
            "total_recovered = EXCLUDED.total_recovered," +
            "date_maj = EXCLUDED.date_maj;";

    private final String INSERT_UPDATE_SQL_COUNTRY = "INSERT INTO countries " +
            "(country,country_code,slug,new_confirmed, total_confirmed, new_deaths, total_deaths, new_recovered, total_recovered, date_maj) " +
            "VALUES " +
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (country) DO UPDATE " +
            "SET new_confirmed = EXCLUDED.new_confirmed, " +
            "total_confirmed = EXCLUDED.total_confirmed," +
            "new_deaths = EXCLUDED.new_deaths, " +
            "total_deaths = EXCLUDED.total_deaths," +
            "new_recovered = EXCLUDED.new_recovered," +
            "total_recovered = EXCLUDED.total_recovered," +
            "date_maj = EXCLUDED.date_maj;";

    private Connection connect() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, password);

            if (conn != null) {
                System.out.println("Connection success");
            } else {
                System.out.println("Failed to make connection!");
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        return conn;
    }

    public void createTable() throws SQLException {

        try (// Step 2:Create a statement using connection object
             Statement statement = connection.createStatement();) {

            // Step 3: Execute the query or update query
            statement.execute(CREATE_TABLE);
        } catch (SQLException e) {
            // print SQL exception information
            printSQLException(e);
        }

    }

    public void inserOrUpdatetIntoGlobal(long new_confirmed, long total_confirmed,
                                         long new_deaths, long total_deaths,
                                         long new_recovered, long total_recovered,
                                         LocalDateTime date) throws SQLException {

        try (// Step 2:Create a statement using connection object
             PreparedStatement preparedStatement = connection.prepareStatement(INSERT_UPDATE_SQL)) {
            preparedStatement.setInt(1, 1);
            preparedStatement.setLong(2, new_confirmed);
            preparedStatement.setLong(3, total_confirmed);
            preparedStatement.setLong(4, new_deaths);
            preparedStatement.setLong(5, total_deaths);
            preparedStatement.setLong(6, new_recovered);
            preparedStatement.setLong(7, total_recovered);
            preparedStatement.setTimestamp(8, Timestamp.valueOf(date));

            // Step 3: Execute the query or update query
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            // print SQL exception information
            printSQLException(e);
        }

    }

    public void inserOrUpdatetIntoCountry(String country, String countryCode, String slug,
                                          long new_confirmed, long total_confirmed,
                                          long new_deaths, long total_deaths,
                                          long new_recovered, long total_recovered,
                                          LocalDateTime date) throws SQLException {

        // Step 1: Establishing a Connection
        try (// Step 2:Create a statement using connection object
             PreparedStatement preparedStatement = connection.prepareStatement(INSERT_UPDATE_SQL_COUNTRY)) {

            preparedStatement.setString(1, country);
            preparedStatement.setString(2, countryCode);
            preparedStatement.setString(3, slug);
            preparedStatement.setLong(4, new_confirmed);
            preparedStatement.setLong(5, total_confirmed);
            preparedStatement.setLong(6, new_deaths);
            preparedStatement.setLong(7, total_deaths);
            preparedStatement.setLong(8, new_recovered);
            preparedStatement.setLong(9, total_recovered);
            preparedStatement.setTimestamp(10, Timestamp.valueOf(date));

            // Step 3: Execute the query or update query
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            // print SQL exception information
            printSQLException(e);
        }

    }

    public static void printSQLException(SQLException ex) {
        for (Throwable e : ex) {
            if (e instanceof SQLException) {
                e.printStackTrace(System.err);
                System.err.println("SQLState: " + ((SQLException) e).getSQLState());
                System.err.println("Error Code: " + ((SQLException) e).getErrorCode());
                System.err.println("Message: " + e.getMessage());
                Throwable t = ex.getCause();
                while (t != null) {
                    System.out.println("Cause: " + t);
                    t = t.getCause();
                }
            }
        }
    }

    public static final Database getInstance()
    {
        return instance;
    }

    public Connection getConnection() {
        if(connection == null){
            connection = connect();
        }
        return connection;
    }

    public void closeConnection() throws SQLException {
        if(connection != null)
            connection.close();
    }
}

