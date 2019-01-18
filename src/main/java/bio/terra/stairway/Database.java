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
 *  flight table - records the flight, its inputs, and its outputs if any
 *  flight log - records the steps of a running flight for recovery
 * This code assumes that the database is created and matches this codes schema
 * expectations. If not, we will crash and burn.
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
    private static String FLIGHT_TABLE = "flight";
    private static String FLIGHT_LOG_TABLE = "flightlog";

    private DataSource dataSource; // may be null, indicating database support is disabled
    private boolean forceCleanStart;

    public Database(DataSource dataSource,
                    boolean forceCleanStart) {

        this.dataSource = dataSource;
        this.forceCleanStart = forceCleanStart;

        if (isDatabaseDisabled()) {
            return;
        }

        // Clean up if need be
        if (forceCleanStart) {
            startClean();
        }
    }

    private boolean isDatabaseDisabled() {
        return (dataSource == null);
    }

    /**
     * Truncate the tables
     */
    private void startClean() {
        final String sqlTruncateTables = "TRUNCATE TABLE " +
                FLIGHT_TABLE + "," +
                FLIGHT_LOG_TABLE;

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {

            statement.executeUpdate(sqlTruncateTables);

        } catch (SQLException ex) {
            throw new DatabaseSetupException("Failed to truncate database tables", ex);
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
                .toString();
    }
}
