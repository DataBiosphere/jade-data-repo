package bio.terra.stairway;

import bio.terra.stairway.exception.DatabaseSetupException;
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
 * If no dataSource is specified, then database support is disabled. The entrypoints all work,
 * but no database operations are performed.
 */
public class Database {
    // Note: these are package accessible for the unit tests
    static String FLIGHT_SCHEMA_VERSION = "0.1.0";
    static String FLIGHT_VERSION_TABLE = "flightversion";
    static String FLIGHT_TABLE = "flight";
    static String FLIGHT_LOG_TABLE = "flightlog";


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

    boolean versionTableExists(Statement statement) throws SQLException {
        String query = "SELECT COUNT(*) AS version_table_count" +
                " FROM information_schema.tables WHERE table_schema = '" +
                (schemaName == null ? "public" : schemaName) +
                "' AND table_name = '" + flightVersionTableName + "'";

        ResultSet rs = statement.executeQuery(query);
        if (rs.next()) {
            int tableCount = rs.getInt("version_table_count");
            return (tableCount == 1);
        } else {
            throw new DatabaseSetupException("Invalid result from information_schema query");
        }
    }

    String readDatabaseSchemaVersion(Statement statement) throws SQLException {
        String version;
        ResultSet rs = statement.executeQuery("SELECT version FROM " + flightVersionTableName);
        if (rs.next()) {
            return rs.getString("version");
        }
        throw new DatabaseSetupException("Schema version not found");
    }

    String readDatabaseSchemaCreateTime(Statement statement) throws SQLException {
        String version;
        ResultSet rs = statement.executeQuery("SELECT create_time::text AS createtime FROM " + flightVersionTableName);
        if (rs.next()) {
            return rs.getString("createtime");
        }
        throw new DatabaseSetupException("Schema create time not found");
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
                    "CREATE TABLE " + flightVersionTableName +
                            "(version varchar(50)," +
                            " create_time timestamp)");

            statement.executeUpdate(
                    "INSERT INTO " + flightVersionTableName +
                            " VALUES('" + FLIGHT_SCHEMA_VERSION + "', CURRENT_TIMESTAMP)");

            statement.executeUpdate(
                    "CREATE TABLE " + flightTableName +
                            "(flightid CHAR(36) PRIMARY KEY," +
                            " submit_time TIMESTAMP NOT NULL," +
                            " input_paramters JSONB," +
                            " completed_time TIMESTAMP," +
                            " output_parameters JSONB," +
                            " succeeded BOOLEAN," +
                            " error_message TEXT)");

            statement.executeUpdate(
                    "CREATE TABLE " + flightLogTableName +
                            "(flightid CHAR(36)," +
                            " log_time TIMESTAMP NOT NULL," +
                            " working_paramters JSONB NOT NULL," +
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

    }

    /**
     * Record the flight state right before a step
     */
    public void step(FlightContext flightContext) {
        if (isDatabaseDisabled()) {
            return;
        }

    }

    /**
     * Record completion of a flight and remove the data from the log
     */
    public void complete(FlightContext flightContext) {
        if (isDatabaseDisabled()) {
            return;
        }

    }

    /**
     * Find all incomplete flights and return the context
     */
    public List<FlightContext> recover() {
        if (isDatabaseDisabled()) {
            return new LinkedList<>();
        }

        // TODO: implement this...
        return new LinkedList<>();
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