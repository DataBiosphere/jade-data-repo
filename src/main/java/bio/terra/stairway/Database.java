package bio.terra.stairway;

import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.DatabaseSetupException;
import bio.terra.stairway.exception.FlightException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
@SuppressFBWarnings(
        value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE",
        justification = "The current state is low risk since we own all of the input values. Still," +
        " it would be best practice to fix it.")

public class Database {
    private static String FLIGHT_SCHEMA_VERSION = "0.1.0";
    private static String FLIGHT_VERSION_TABLE = "flightversion";
    private static String FLIGHT_TABLE = "flight";
    private static String FLIGHT_LOG_TABLE = "flightlog";

    // Getters are package scoped for unit tests
    static String getFlightSchemaVersion() {
        return FLIGHT_SCHEMA_VERSION;
    }

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
    private String schemaName;  // may be null
    private String nameStem;    // may be null

    // State used and computed as part of starting up the database.
    // Once the Database object is constructed, these are not changed.
    private String flightTableName;
    private String flightLogTableName;
    private String flightVersionTableName;
    private boolean schemaExists;  // true if the schema exists in the database
    private String schemaVersion;  // version of the schema, if it exists

    public Database(DataSource dataSource,
                    boolean forceCleanStart,
                    String schemaName,
                    String nameStem) {

        this.dataSource = dataSource;
        this.forceCleanStart = forceCleanStart;
        this.schemaName = schemaName;
        this.nameStem = nameStem;

        if (isDatabaseDisabled()) {
            return;
        }

        // Compute the table names
        String namePrefix;
        if (schemaName == null) {
            namePrefix = "";
        } else {
            namePrefix = schemaName + '.';
        }
        if (nameStem != null) {
            namePrefix = namePrefix + nameStem + '_';
        }
        this.flightVersionTableName = namePrefix + FLIGHT_VERSION_TABLE;
        this.flightTableName = namePrefix + FLIGHT_TABLE;
        this.flightLogTableName = namePrefix + FLIGHT_LOG_TABLE;

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
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {

            statement.executeUpdate(
                    "DROP TABLE IF EXISTS " +
                            flightVersionTableName + "," +
                            flightTableName + "," +
                            flightLogTableName);

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
            // TODO: make this more subtle. For now if the version doesn't exactly
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
        String query = "SELECT COUNT(*) AS version_table_count" +
                " FROM information_schema.tables WHERE table_schema = '" +
                (schemaName == null ? "public" : schemaName) +
                "' AND table_name = '" +
                (nameStem == null ? "" : nameStem + '_') + FLIGHT_VERSION_TABLE + "'";

        try (ResultSet rs = statement.executeQuery(query)) {
            if (rs.next()) {
                int tableCount = rs.getInt("version_table_count");
                return (tableCount == 1);
            } else {
                throw new DatabaseSetupException("Invalid result from information_schema query");
            }
        }
    }

    String readDatabaseSchemaVersion(Statement statement) throws SQLException {
        try (ResultSet rs = statement.executeQuery("SELECT version FROM " + flightVersionTableName)) {
            if (rs.next()) {
                return rs.getString("version");
            }
            throw new DatabaseSetupException("Schema version not found");
        }
    }

    String readDatabaseSchemaCreateTime(Statement statement) throws SQLException {
        try (ResultSet rs = statement.executeQuery(
                "SELECT create_time::text AS createtime FROM " + flightVersionTableName)) {
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

            if (schemaName != null) {
                statement.executeUpdate("CREATE SCHEMA IF NOT EXISTS " + schemaName);
            }

            statement.executeUpdate(
                    "CREATE TABLE " + flightVersionTableName +
                            "(version VARCHAR(50)," +
                            " create_time TIMESTAMP)");

            statement.executeUpdate(
                    "INSERT INTO " + flightVersionTableName +
                            " VALUES('" + FLIGHT_SCHEMA_VERSION + "', CURRENT_TIMESTAMP)");

            statement.executeUpdate(
                    "CREATE TABLE " + flightTableName +
                            "(flightid VARCHAR(36) PRIMARY KEY," +
                            " submit_time TIMESTAMP NOT NULL," +
                            " class_name TEXT NOT NULL," +
                            " input_parameters JSONB," +
                            " completed_time TIMESTAMP," +
                            " output_parameters JSONB," +
                            " succeeded BOOLEAN," +
                            " error_message TEXT)");

            statement.executeUpdate(
                    "CREATE TABLE " + flightLogTableName +
                            "(flightid VARCHAR(36)," +
                            " log_time TIMESTAMP NOT NULL," +
                            " working_parameters JSONB NOT NULL," +
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

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {

            statement.executeUpdate(
                    "INSERT INTO " + flightTableName +
                            "(flightId, submit_time, class_name, input_parameters) VALUES ('" +
                            flightContext.getFlightId() + "', CURRENT_TIMESTAMP, '" +
                            flightContext.getFlightClassName() + "', '" +
                            flightContext.getInputParameters().toJson() + "')");

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

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {

            statement.executeUpdate(
                    "INSERT INTO " + flightLogTableName +
                            "(flightid, log_time, working_parameters, step_index, doing, succeeded, error_message)" +
                            " VALUES ('" +
                            flightContext.getFlightId() + "', CURRENT_TIMESTAMP, '" +
                            flightContext.getWorkingMap().toJson() + "'," +
                            flightContext.getStepIndex() + "," +
                            boolString(flightContext.isDoing()) + "," +
                            boolString(flightContext.getResult().isSuccess()) + ", '" +
                            flightContext.getResult().getErrorMessage() + "')");

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

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);

            // Make the update idempotent; that is, only do it if it has not already been done by
            // including the "succeeded IS NULL" predicate.
            statement.executeUpdate(
                    "UPDATE " + flightTableName +
                            " SET completed_time = CURRENT_TIMESTAMP," +
                            " output_parameters = '" + flightContext.getWorkingMap().toJson() +
                            "', succeeded = " + boolString(flightContext.getResult().isSuccess()) +
                            ", error_message = '" + flightContext.getResult().getErrorMessage() +
                            "' WHERE flightid = '" + flightContext.getFlightId() +
                            "' AND succeeded IS NULL");

            // The delete is harmless if it has been done before. We just won't find anything.
            statement.executeUpdate(
                    "DELETE FROM " + flightLogTableName +
                            " WHERE flightid = '" + flightContext.getFlightId() + "'");

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

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ) {

            try (ResultSet rs = statement.executeQuery("SELECT flightid, class_name, input_parameters" +
                    " FROM " + flightTableName +
                    " WHERE succeeded IS NULL")) {
                while (rs.next()) {
                    SafeHashMap inputParameters = new SafeHashMap();
                    inputParameters.fromJson(rs.getString("input_parameters"));

                    FlightContext flightContext = new FlightContext(inputParameters)
                            .flightId(rs.getString("flightid"))
                            .flightClassName(rs.getString("class_name"));
                    flightList.add(flightContext);
                }
            }

            // Loop through the linked list making a query for each flight. This may not be the most efficient.
            // My reasoning is that the code is more obvious to understand and this is not
            // a performance-critical part of the processing; it happens once at startup.
            for (FlightContext flightContext : flightList) {
                try (ResultSet rsflight = statement.executeQuery(
                        "SELECT working_parameters, step_index, doing, succeeded, error_message" +
                                " FROM (SELECT *, MAX(log_time) OVER (PARTITION BY flightid) AS max_log_time" +
                                " FROM " + flightLogTableName + " WHERE flightid = '" +
                                flightContext.getFlightId() + "') AS S" +
                                " WHERE log_time = max_log_time")) {

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

                        flightContext.stepIndex(rsflight.getInt("step_index"))
                                .doing(rsflight.getBoolean("doing"))
                                .result(stepResult);
                    }
                }
            }

        } catch (SQLException ex) {
            throw new DatabaseOperationException("Failed to get incomplete flight list", ex);
        }

        return flightList;
    }

    private String boolString(boolean value) {
        return (value ? "true" : "false");
    }


    // NOTE: getters are package scoped so that unit tests can access them. They are not intended for
    // general use.
    String getFlightTableName() {
        return flightTableName;
    }

    String getFlightLogTableName() {
        return flightLogTableName;
    }

    String getFlightVersionTableName() {
        return flightVersionTableName;
    }

    boolean isSchemaExists() {
        return schemaExists;
    }

    String getSchemaVersion() {
        return schemaVersion;
    }


    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                .append("dataSource", dataSource)
                .append("forceCleanStart", forceCleanStart)
                .append("schemaName", schemaName)
                .append("nameStem", nameStem)
                .append("flightTableName", flightTableName)
                .append("flightLogTableName", flightLogTableName)
                .append("flightVersionTableName", flightVersionTableName)
                .append("schemaExists", schemaExists)
                .append("schemaVersion", schemaVersion)
                .toString();
    }
}
