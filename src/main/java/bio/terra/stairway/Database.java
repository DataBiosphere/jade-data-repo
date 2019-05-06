package bio.terra.stairway;

import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.DatabaseSetupException;
import bio.terra.stairway.exception.FlightException;
import bio.terra.stairway.exception.FlightNotFoundException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.storage.StorageException;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.postgresql.util.PSQLException;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

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
 */
public class Database {
    private static String FLIGHT_TABLE = "flight";
    private static String FLIGHT_LOG_TABLE = "flightlog";

    private ObjectMapper objectMapper;

    private DataSource dataSource;
    private boolean forceCleanStart;

    public Database(DataSource dataSource, boolean forceCleanStart) {

        this.dataSource = dataSource;
        this.forceCleanStart = forceCleanStart;

        // Clean up if need be
        if (forceCleanStart) {
            startClean();
        }
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
        final String sqlInsertFlight =
                "INSERT INTO " + FLIGHT_TABLE +
                        " (flightId, submit_time, class_name, input_parameters, status)" +
                        "VALUES (:flightid, CURRENT_TIMESTAMP, :class_name, :inputs, :status)";

        try (Connection connection = dataSource.getConnection();
             NamedParameterPreparedStatement statement =
                     new NamedParameterPreparedStatement(connection, sqlInsertFlight)) {

            statement.setString("flightid", flightContext.getFlightId());
            statement.setString("class_name", flightContext.getFlightClassName());
            statement.setString("inputs", flightContext.getInputParameters().toJson());
            statement.setString("status", flightContext.getFlightStatus().name());
            statement.getPreparedStatement().executeUpdate();

        } catch (SQLException ex) {
            throw new DatabaseOperationException("Failed to create database tables", ex);
        }
    }

    /**
     * Record the flight state right before a step
     */
    public void step(FlightContext flightContext) {
        final String sqlInsertFlightLog =
                "INSERT INTO " + FLIGHT_LOG_TABLE +
                        "(flightid, log_time, working_parameters, step_index, doing," +
                        " succeeded, exception, status)" +
                        " VALUES (:flightid, CURRENT_TIMESTAMP, :working_map, :step_index, :doing," +
                        " :succeeded, :exception, :status)";

        try (Connection connection = dataSource.getConnection();
             NamedParameterPreparedStatement statement =
                     new NamedParameterPreparedStatement(connection, sqlInsertFlightLog)) {

            statement.setString("flightid", flightContext.getFlightId());
            statement.setString("working_map", flightContext.getWorkingMap().toJson());
            statement.setInt("step_index", flightContext.getStepIndex());
            statement.setBoolean("doing", flightContext.isDoing());
            statement.setBoolean("succeeded", flightContext.getResult().isSuccess());
            statement.setString("exception", getExceptionJson(flightContext));
            statement.setString("status", flightContext.getFlightStatus().name());

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
        // Make the update idempotent; that is, only do it if the status is RUNNING
        final String sqlUpdateFlight =
                "UPDATE " + FLIGHT_TABLE +
                        " SET completed_time = CURRENT_TIMESTAMP," +
                        " output_parameters = :output_parameters," +
                        " status = :status," +
                        " exception = :exception" +
                        " WHERE flightid = :flightid AND status = 'RUNNING'";

        // The delete is harmless if it has been done before. We just won't find anything.
        final String sqlDeleteFlightLog = "DELETE FROM " + FLIGHT_LOG_TABLE + " WHERE flightid = :flightid";

        try (Connection connection = dataSource.getConnection();
             NamedParameterPreparedStatement statement =
                     new NamedParameterPreparedStatement(connection, sqlUpdateFlight);
             NamedParameterPreparedStatement deleteStatement =
                     new NamedParameterPreparedStatement(connection, sqlDeleteFlightLog)) {

            connection.setAutoCommit(false);

            statement.setString("output_parameters", flightContext.getWorkingMap().toJson());
            statement.setString("status", flightContext.getFlightStatus().name());
            statement.setString("exception", getExceptionJson(flightContext));
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
        List<FlightContext> flightList = new LinkedList<>();

        final String sqlActiveFlights = "SELECT flightid, class_name, input_parameters" +
                " FROM " + FLIGHT_TABLE +
                " WHERE status = 'RUNNING'";

        final String sqlLastFlightLog = "SELECT working_parameters, step_index, doing," +
                " succeeded, exception, status" +
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
                                    getExceptionFromJson(rsflight.getString("exception")));
                        }

                        flightContext.getWorkingMap().fromJson(rsflight.getString("working_parameters"));

                        flightContext.setStepIndex(rsflight.getInt("step_index"));
                        flightContext.setDoing(rsflight.getBoolean("doing"));
                        flightContext.setResult(stepResult);
                        FlightStatus flightStatus = FlightStatus.valueOf(rsflight.getString("status"));
                        flightContext.setFlightStatus(flightStatus);
                    }
                }
            }

        } catch (SQLException ex) {
            throw new DatabaseOperationException("Failed to get incomplete flight list", ex);
        }

        return flightList;
    }

    /**
     * Return flight state for a single flight
     *
     * @param flightId
     * @return FlightState for the flight
     */
    public FlightState getFlightState(String flightId) {
        final String sqlOneFlight = "SELECT flightid, submit_time, input_parameters," +
                " completed_time, output_parameters, status, exception" +
                " FROM " + FLIGHT_TABLE +
                " WHERE flightid = :flightid";

        try (Connection connection = dataSource.getConnection();
             NamedParameterPreparedStatement oneFlightStatement =
                     new NamedParameterPreparedStatement(connection, sqlOneFlight)) {

            oneFlightStatement.setString("flightid", flightId);

            try (ResultSet rs = oneFlightStatement.getPreparedStatement().executeQuery()) {
                List<FlightState> flightStateList = makeFlightStateList(rs);
                if (flightStateList.size() == 0) {
                    throw new FlightNotFoundException("Flight not found: " + flightId);
                }
                if (flightStateList.size() > 1) {
                    throw new DatabaseOperationException("Multiple flights with the same id?!");
                }
                return flightStateList.get(0);
            }

        } catch (SQLException ex) {
            throw new DatabaseOperationException("Failed to get flight", ex);
        }
    }

    public List<FlightState> getFlights(int offset, int limit) {
        final String sqlFlightRange = "SELECT flightid, submit_time, input_parameters," +
                " completed_time, output_parameters, status, exception" +
                " FROM " + FLIGHT_TABLE +
                " ORDER BY submit_time" +
                " LIMIT :limit OFFSET :offset";

        try (Connection connection = dataSource.getConnection();
             NamedParameterPreparedStatement flightRangeStatement =
                     new NamedParameterPreparedStatement(connection, sqlFlightRange)) {

            flightRangeStatement.setInt("limit", limit);
            flightRangeStatement.setInt("offset", offset);

            try (ResultSet rs = flightRangeStatement.getPreparedStatement().executeQuery()) {
                List<FlightState> flightStateList = makeFlightStateList(rs);
                return flightStateList;
            }

        } catch (SQLException ex) {
            throw new DatabaseOperationException("Failed to get flights", ex);
        }
    }

    private List<FlightState> makeFlightStateList(ResultSet rs) throws SQLException {
        List<FlightState> flightStateList = new ArrayList<>();

        while (rs.next()) {
            FlightState flightState = new FlightState();

            // Flight data that is always present
            flightState.setFlightId(rs.getString("flightid"));
            flightState.setFlightStatus(FlightStatus.valueOf(rs.getString("status")));
            flightState.setSubmitted(rs.getTimestamp("submit_time").toInstant());

            FlightMap inputParameters = new FlightMap();
            inputParameters.fromJson(rs.getString("input_parameters"));
            flightState.setInputParameters(inputParameters);


            // Only populate the optional fields if the flight is done; that is, not RUNNING
            if (flightState.getFlightStatus() == FlightStatus.RUNNING) {
                flightState.setCompleted(Optional.empty());
                flightState.setException(Optional.empty());
                flightState.setResultMap(Optional.empty());
            } else {
                // If the optional flight data is present, then we fill it in
                flightState.setCompleted(Optional.ofNullable(rs.getTimestamp("completed_time").toInstant()));
                flightState.setException(Optional.ofNullable(
                    getExceptionFromJson(rs.getString("exception"))));
                String outputParamsJson = rs.getString("output_parameters");
                if (outputParamsJson == null) {
                    flightState.setResultMap(Optional.empty());
                } else {
                    FlightMap outputParameters = new FlightMap();
                    outputParameters.fromJson(outputParamsJson);
                    flightState.setResultMap(Optional.of(outputParameters));
                }
            }

            flightStateList.add(flightState);
        }

        return flightStateList;
    }

    private String getExceptionJson(FlightContext flightContext) {
        try {
            String exceptionJson = null;
            if (flightContext.getResult().getException().isPresent()) {
                Exception exception = flightContext.getResult().getException().get();

                if (!isSafeToDeserialize(exception)) {
                    exception = rewriteException(exception);
                }
                exceptionJson = getObjectMapper().writeValueAsString(exception);
            }
            return exceptionJson;
        } catch (JsonProcessingException ex) {
            throw new DatabaseOperationException("Failed to convert exception to JSON", ex);
        }
    }

    private boolean isSafeToDeserialize(Throwable inException) {
        if (inException != null) {
            if (!isSafeToDeserialize(inException.getCause())) {
                // If someone under us isn't safe, it doesn't matter what we are.
                return false;
            }

            if (inException instanceof StorageException ||
                inException instanceof BigQueryException ||
                inException instanceof  PSQLException ||
                inException instanceof URISyntaxException) {
                return false;
            }
        }

        return true;
    }

    private Exception rewriteException(Throwable inException) {
        // It turns out you cannot rewrite a cause in an existing exception
        // stack, so to make the exception safe for serialize/deserialize, we
        // have to rewrite the stack into flight exceptions.
        if (inException != null) {
            Exception under = rewriteException(inException.getCause());
            return new FlightException(inException.toString(), under);
        }
        return null;
    }

    private Exception getExceptionFromJson(String exceptionJson) {
        try {
            if (exceptionJson == null) {
                return null;
            }
            return getObjectMapper().readValue(exceptionJson, Exception.class);
        } catch (IOException ex) {
            throw new DatabaseOperationException("Failed to convert JSON to exception", ex);
        }
    }

    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper()
                .registerModule(new ParameterNamesModule())
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule())
                .enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }
        return objectMapper;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                .append("dataSource", dataSource)
                .append("forceCleanStart", forceCleanStart)
                .toString();
    }
}
