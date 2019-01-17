package bio.terra.stairway;

import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.DatabaseSetupException;
import bio.terra.stairway.exception.FlightException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

/**
 * The general layout of the database is:
 *  version - records the version number of the schema
 *  flight table - records the flight, its inputs, and its outputs if any
 *  flight log - records the steps of a running flight for recovery
 *
 * May want to split this into an interface and an implementation. This implementation
 * assumes Postgres.
 *
 * The constructor performs initialization, so it depends on the dataSource being configured
 * before this is constructed. Be aware of this if both Stairway and DataSource are created as
 * beans during Spring startup.
 *
 * When the constructor completes, the database is ready for use. Note that recovery
 * is not part of construction. It just makes sure that the database is ready.
 *
 * If dataSource is null, then database support is disabled. The entrypoints all work,
 * but no database operations are performed.
 */
public class Database {
    private static String FLIGHT_SCHEMA_VERSION = "0.1.0";
    private static String FLIGHT_VERSION_TABLE = "flightversion";
    private static String FLIGHT_TABLE = "flight";
    private static String FLIGHT_LOG_TABLE = "flightlog";

    // Getters are package scoped for unit tests
    static String getFlightVersionTable() {
        return FLIGHT_VERSION_TABLE;
    }

    static String getFlightTable() {
        return FLIGHT_TABLE;
    }

    static String getFlightLogTable() {
        return FLIGHT_LOG_TABLE;
    }

    private DataSource dataSource; // may be null, indicating database support is disabled
    private boolean forceCleanStart;

    // State used and computed as part of starting up the database.
    // Once the Database object is constructed, these are not changed.
    private boolean schemaExists;  // true if the schema exists in the database
    private String schemaVersion;  // version of the schema, if it exists

    public Database(DataSource dataSource,
                    boolean forceCleanStart) {

        this.dataSource = dataSource;
        this.forceCleanStart = forceCleanStart;

        if (isDatabaseDisabled()) {
            return;
        }

        // Configure the database
        if (forceCleanStart) {
            startClean();
        } else {
            startDirty();
        }
    }

    private boolean isDatabaseDisabled() {
        return (dataSource == null);
    }

    /**
     * If tables exist, drop them and re-create them with the version in this code.
     */
    private void startClean() {
        final String sqlDropTables = "DROP TABLE IF EXISTS " +
                FLIGHT_VERSION_TABLE + "," +
                FLIGHT_TABLE + "," +
                FLIGHT_LOG_TABLE;

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {

            statement.executeUpdate(sqlDropTables);

        } catch (SQLException ex) {
            throw new DatabaseSetupException("Failed to create database tables", ex);
        }

        // Create the schema from scratch
        create();
    }

    /**
     * If tables exist, read the schema version. Throw if the version is incompatible.
     * If tables do not exist, create them.
     * NOTE: this method does not (and cannot) initiate recovery. That is a separate
     * operation driven from the Staircase class.
     */
    private void startDirty() {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {

            schemaExists = versionTableExists(statement);
            if (!schemaExists) {
                create();
                return;
            }

            // Schema exists. Pull out the schema version.
            // TODO: this will get factored out and we will use liquibase
            // match we barf.
            schemaVersion = readDatabaseSchemaVersion(statement);
            if (!StringUtils.equals(schemaVersion, FLIGHT_SCHEMA_VERSION)) {
                throw new DatabaseSetupException("Database schema version " + schemaVersion +
                        "is incompatible with software schema version " + FLIGHT_SCHEMA_VERSION);
            }

        } catch (SQLException ex) {
            throw new DatabaseSetupException("Failed to get schema version state", ex);
        }
    }

    // Database accessors - package scoped so unit tests can use them
    boolean versionTableExists(Statement statement) throws SQLException {
        final String sqlInfoSchemaLookup = "SELECT COUNT(*) AS version_table_count" +
                " FROM information_schema.tables" +
                " WHERE table_schema = 'public' AND table_name = '" +
                FLIGHT_VERSION_TABLE + "'";

        try (ResultSet rs = statement.executeQuery(sqlInfoSchemaLookup)) {
            if (rs.next()) {
                int tableCount = rs.getInt("version_table_count");
                return (tableCount == 1);
            } else {
                throw new DatabaseSetupException("Invalid result from information_schema query");
            }
        }
    }

    String readDatabaseSchemaVersion(Statement statement) throws SQLException {
        try (ResultSet rs = statement.executeQuery("SELECT version FROM " + FLIGHT_VERSION_TABLE)) {
            if (rs.next()) {
                return rs.getString("version");
            }
            throw new DatabaseSetupException("Schema version not found");
        }
    }

    String readDatabaseSchemaCreateTime(Statement statement) throws SQLException {
        try (ResultSet rs = statement.executeQuery(
                "SELECT create_time::text AS createtime FROM " + FLIGHT_VERSION_TABLE)) {
            if (rs.next()) {
                return rs.getString("createtime");
            }
            throw new DatabaseSetupException("Schema create time not found");
        }
    }

    /**
     * Create the schema and store its version number
     */
    private void create() {
        if (isDatabaseDisabled()) {
            return;
        }

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);

            statement.executeUpdate(
                    "CREATE TABLE " + FLIGHT_VERSION_TABLE +
                            "(version VARCHAR(50)," +
                            " create_time TIMESTAMP)");

            statement.executeUpdate(
                    "INSERT INTO " + FLIGHT_VERSION_TABLE +
                            " VALUES('" + FLIGHT_SCHEMA_VERSION + "', CURRENT_TIMESTAMP)");

            statement.executeUpdate(
                    "CREATE TABLE " + FLIGHT_TABLE +
                            "(flightid VARCHAR(36) PRIMARY KEY," +
                            " submit_time TIMESTAMP NOT NULL," +
                            " class_name TEXT NOT NULL," +
                            " input_parameters TEXT," +
                            " completed_time TIMESTAMP," +
                            " output_parameters TEXT," +
                            " succeeded BOOLEAN," +
                            " error_message TEXT)");

            statement.executeUpdate(
                    "CREATE TABLE " + FLIGHT_LOG_TABLE +
                            "(flightid VARCHAR(36)," +
                            " log_time TIMESTAMP NOT NULL," +
                            " working_parameters TEXT NOT NULL," +
                            " step_index INTEGER NOT NULL," +
                            " doing BOOLEAN NOT NULL," + // true = forward; false = backward
                            " succeeded BOOLEAN," + // null = not done; true = success; false = failure
                            " error_message TEXT)"); // failure reason(s)

            connection.commit();

            schemaExists = true;
            schemaVersion = FLIGHT_SCHEMA_VERSION;

        } catch (SQLException ex) {
            throw new DatabaseSetupException("Failed to create database tables", ex);
        }
    }

    /**
     * Record a new flight
     */
    public void submit(FlightContext flightContext) {
        if (isDatabaseDisabled()) {
            return;
        }

        final String sqlInsertFlight =
                "INSERT INTO " + FLIGHT_TABLE +
                        " (flightId, submit_time, class_name, input_parameters)" +
                        "VALUES (:flightid, CURRENT_TIMESTAMP, :class_name, :inputs)";

        try (Connection connection = dataSource.getConnection();
             NamedParameterPreparedStatement statement =
                     new NamedParameterPreparedStatement(connection, sqlInsertFlight)) {

            statement.setString("flightid", flightContext.getFlightId());
            statement.setString("class_name", flightContext.getFlightClassName());
            statement.setString("inputs", flightContext.getInputParameters().toJson());
            statement.getPreparedStatement().executeUpdate();

        } catch (SQLException ex) {
            throw new DatabaseOperationException("Failed to create database tables", ex);
        }
    }

    /**
     * Record the flight state right before a step
     */
    public void step(FlightContext flightContext) {
        if (isDatabaseDisabled()) {
            return;
        }

        final String sqlInsertFlightLog =
                "INSERT INTO " + FLIGHT_LOG_TABLE +
                        "(flightid, log_time, working_parameters, step_index, doing, succeeded, error_message)" +
                        " VALUES (:flightid, CURRENT_TIMESTAMP, :working_map," +
                        " :step_index, :doing, :succeeded, :error_message)";

        try (Connection connection = dataSource.getConnection();
             NamedParameterPreparedStatement statement =
                     new NamedParameterPreparedStatement(connection, sqlInsertFlightLog)) {

            statement.setString("flightid", flightContext.getFlightId());
            statement.setString("working_map", flightContext.getWorkingMap().toJson());
            statement.setInt("step_index", flightContext.getStepIndex());
            statement.setBoolean("doing", flightContext.isDoing());
            statement.setBoolean("succeeded", flightContext.getResult().isSuccess());
            statement.setString("error_message", flightContext.getResult().getErrorMessage());

            statement.getPreparedStatement().executeUpdate();

        } catch (SQLException ex) {
            throw new DatabaseOperationException("Failed to log step", ex);
        }

    }

    /**
     * Record completion of a flight and remove the data from the log
     * This is idempotent; repeated execution will work properly.
     */
    public void complete(FlightContext flightContext) {
        if (isDatabaseDisabled()) {
            return;
        }

        // Make the update idempotent; that is, only do it if it has not already been done by
        // including the "succeeded IS NULL" predicate.
        final String sqlUpdateFlight =
                "UPDATE " + FLIGHT_TABLE +
                        " SET completed_time = CURRENT_TIMESTAMP," +
                        " output_parameters = :output_parameters," +
                        " succeeded = :succeeded," +
                        " error_message = :error_message" +
                        " WHERE flightid = :flightid AND succeeded IS NULL";


        // The delete is harmless if it has been done before. We just won't find anything.
        final String sqlDeleteFlightLog = "DELETE FROM " + FLIGHT_LOG_TABLE + " WHERE flightid = :flightid";


        try (Connection connection = dataSource.getConnection();
             NamedParameterPreparedStatement statement =
                     new NamedParameterPreparedStatement(connection, sqlUpdateFlight);
             NamedParameterPreparedStatement deleteStatement =
                     new NamedParameterPreparedStatement(connection, sqlDeleteFlightLog)) {

            connection.setAutoCommit(false);

            statement.setString("output_parameters", flightContext.getWorkingMap().toJson());
            statement.setBoolean("succeeded", flightContext.getResult().isSuccess());
            statement.setString("error_message", flightContext.getResult().getErrorMessage());
            statement.setString("flightid", flightContext.getFlightId());
            statement.getPreparedStatement().executeUpdate();

            deleteStatement.setString("flightid", flightContext.getFlightId());
            statement.getPreparedStatement().executeUpdate();

            connection.commit();

        } catch (SQLException ex) {
            throw new DatabaseOperationException("Failed to complete flight", ex);
        }

    }

    /**
     * Find all incomplete flights and return the context
     */
    public List<FlightContext> recover() {
        if (isDatabaseDisabled()) {
            return new LinkedList<>();
        }

        List<FlightContext> flightList = new LinkedList<>();

        final String sqlActiveFlights = "SELECT flightid, class_name, input_parameters" +
                " FROM " + FLIGHT_TABLE +
                " WHERE succeeded IS NULL";

        final String sqlLastFlightLog = "SELECT working_parameters, step_index, doing, succeeded, error_message" +
                " FROM " + FLIGHT_LOG_TABLE +
                " WHERE flightid = :flightid AND log_time = " +
                " (SELECT MAX(log_time) FROM " + FLIGHT_LOG_TABLE + " WHERE flightid = :flightid2)";


        try (Connection connection = dataSource.getConnection();
             NamedParameterPreparedStatement activeFlightsStatement =
                     new NamedParameterPreparedStatement(connection, sqlActiveFlights);
             NamedParameterPreparedStatement lastFlightLogStatement =
                     new NamedParameterPreparedStatement(connection, sqlLastFlightLog)) {

            try (ResultSet rs = activeFlightsStatement.getPreparedStatement().executeQuery()) {
                while (rs.next()) {
                    FlightMap inputParameters = new FlightMap();
                    inputParameters.fromJson(rs.getString("input_parameters"));

                    FlightContext flightContext = new FlightContext(inputParameters,
                            rs.getString("class_name"));
                    flightContext.setFlightId(rs.getString("flightid"));
                    flightList.add(flightContext);
                }
            }

            // Loop through the linked list making a query for each flight. This may not be the most efficient.
            // My reasoning is that the code is more obvious to understand and this is not
            // a performance-critical part of the processing; it happens once at startup.
            for (FlightContext flightContext : flightList) {

                lastFlightLogStatement.setString("flightid", flightContext.getFlightId());
                lastFlightLogStatement.setString("flightid2", flightContext.getFlightId());

                try (ResultSet rsflight = lastFlightLogStatement.getPreparedStatement().executeQuery()) {

                    // There may not be any log entries for a given flight. That happens if we fail after
                    // submit and before the first step. The defaults for flight context are correct for that
                    // case, so there is nothing left to do here.
                    if (rsflight.next()) {
                        StepResult stepResult;
                        if (rsflight.getBoolean("succeeded")) {
                            stepResult = StepResult.getStepResultSuccess();
                        } else {
                            stepResult = new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL,
                                    new FlightException(rsflight.getString("error_message")));
                        }

                        flightContext.getWorkingMap().fromJson(rsflight.getString("working_parameters"));

                        flightContext.setStepIndex(rsflight.getInt("step_index"));
                        flightContext.setDoing(rsflight.getBoolean("doing"));
                        flightContext.setResult(stepResult);
                    }
                }
            }

        } catch (SQLException ex) {
            throw new DatabaseOperationException("Failed to get incomplete flight list", ex);
        }

        return flightList;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                .append("dataSource", dataSource)
                .append("forceCleanStart", forceCleanStart)
                .append("schemaExists", schemaExists)
                .append("schemaVersion", schemaVersion)
                .toString();
    }
}
