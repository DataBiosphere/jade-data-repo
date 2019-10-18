package bio.terra.stairway;

import bio.terra.app.configuration.StairwayJdbcConfiguration;
import bio.terra.stairway.exception.DatabaseOperationException;
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
import org.postgresql.util.PSQLException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The general layout of the stairway database is:
 * flight table - records the flight, its inputs, and its outputs if any
 * flight log - records the steps of a running flight for recovery
 * This code assumes that the database is created and matches this codes schema
 * expectations. If not, we will crash and burn.
 * <p>
 * May want to split this into an interface and an implementation. This implementation
 * assumes Postgres.
 * <p>
 * The constructor performs initialization, so it depends on the dataSource being configured
 * before this is constructed. Be aware of this if both Stairway and DataSource are created as
 * beans during Spring startup.
 * <p>
 * When the constructor completes, the database is ready for use. Note that recovery
 * is not part of construction. It just makes sure that the database is ready.
 */
@Repository
public class FlightDao {
    private static String FLIGHT_TABLE = "flight";
    private static String FLIGHT_LOG_TABLE = "flightlog";

    private static ObjectMapper objectMapper;
    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final StairwayJdbcConfiguration jdbcConfiguration;

    public FlightDao(StairwayJdbcConfiguration jdbcConfiguration) {
        this.jdbcConfiguration = jdbcConfiguration;
        jdbcTemplate = new NamedParameterJdbcTemplate(jdbcConfiguration.getDataSource());
    }

    /**
     * Truncate the tables
     */
    public void startClean() {
        final String sqlTruncateTables = "TRUNCATE TABLE " +
            FLIGHT_TABLE + "," +
            FLIGHT_LOG_TABLE;
        new JdbcTemplate(jdbcConfiguration.getDataSource()).execute(sqlTruncateTables);
    }

    /**
     * Record a new flight
     */
    public void submit(FlightContext flightContext) {
        final String sqlInsertFlight =
            "INSERT INTO " + FLIGHT_TABLE +
                " (flightId, submit_time, class_name, input_parameters, status, owner_id, owner_email)" +
                "VALUES (:flightid, CURRENT_TIMESTAMP, :class_name, :inputs, :status, :subject, :email)";

        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("flightid", flightContext.getFlightId())
            .addValue("class_name", flightContext.getFlightClassName())
            .addValue("inputs", flightContext.getInputParameters().toJson())
            .addValue("status", flightContext.getFlightStatus().name())
            .addValue("subject", flightContext.getUser().getSubjectId())
            .addValue("email", flightContext.getUser().getName());


        jdbcTemplate.update(sqlInsertFlight, params);
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


        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("flightid", flightContext.getFlightId())
            .addValue("working_map", flightContext.getWorkingMap().toJson())
            .addValue("step_index", flightContext.getStepIndex())
            .addValue("doing", flightContext.isDoing())
            .addValue("succeeded", flightContext.getResult().isSuccess())
            .addValue("exception", getExceptionJson(flightContext))
            .addValue("status", flightContext.getFlightStatus().name());

        jdbcTemplate.update(sqlInsertFlightLog, params);

    }

    /**
     * Record completion of a flight and remove the data from the log
     * This is idempotent; repeated execution will work properly.
     */
    @Transactional(propagation = Propagation.REQUIRED)
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

        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("output_parameters", flightContext.getWorkingMap().toJson())
            .addValue("status", flightContext.getFlightStatus().name())
            .addValue("exception", getExceptionJson(flightContext))
            .addValue("flightid", flightContext.getFlightId());

        jdbcTemplate.update(sqlUpdateFlight, params);

        params = new MapSqlParameterSource()
            .addValue("flightid", flightContext.getFlightId());
        jdbcTemplate.update(sqlDeleteFlightLog, params);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public void delete(String flightId) {
        final String sqlDeleteFlightLog = "DELETE FROM " + FLIGHT_LOG_TABLE + " WHERE flightid = :flightid";
        final String sqlDeleteFlight = "DELETE FROM " + FLIGHT_TABLE + " WHERE flightid = :flightid";

        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("flightid", flightId);

        jdbcTemplate.update(sqlDeleteFlightLog, params);
        jdbcTemplate.update(sqlDeleteFlight, params);
    }

    public boolean ownsFlight(String flightId, String subject) {
        final String sqlFlight = "SELECT owner_id" +
            " FROM " + FLIGHT_TABLE +
            " WHERE flightId = :flightid";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("flightid", flightId);
        String flightSubject = jdbcTemplate.queryForObject(sqlFlight, params, String.class);
        return subject.equals(flightSubject);
    }

    /**
     * Find all incomplete flights and return the context
     */
    public List<FlightContext> recover() {

        // todo add req_id
        final String sqlActiveFlights = "SELECT flightid, class_name, input_parameters, owner_id, owner_email" +
            " FROM " + FLIGHT_TABLE +
            " WHERE status = 'RUNNING'";

        final String sqlLastFlightLog = "SELECT working_parameters, step_index, doing," +
            " succeeded, exception, status" +
            " FROM " + FLIGHT_LOG_TABLE +
            " WHERE flightid = :flightid AND log_time = " +
            " (SELECT MAX(log_time) FROM " + FLIGHT_LOG_TABLE + " WHERE flightid = :flightid2)";


        List<FlightContext> flightList = jdbcTemplate.query(sqlActiveFlights, new RowMapper<FlightContext>() {
            public FlightContext mapRow(ResultSet rs, int rowNum) throws SQLException {
                FlightMap inputParameters = new FlightMap();
                inputParameters.fromJson(rs.getString("input_parameters"));

                FlightContext flightContext = new FlightContext(
                    inputParameters,
                    rs.getString("class_name"),
                    new UserRequestInfo()
                        .subjectId(rs.getString("owner_id"))
                        .name(rs.getString("owner_email")));
                flightContext.setFlightId(rs.getString("flightid"));
                return flightContext;
            }
        });

        // Loop through the linked list making a query for each flight. This may not be the most efficient.
        // My reasoning is that the code is more obvious to understand and this is not
        // a performance-critical part of the processing; it happens once at startup.
        for (FlightContext flightContext : flightList) {

            MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("flightid", flightContext.getFlightId())
                .addValue("flightid2", flightContext.getFlightId());

            jdbcTemplate.query(sqlLastFlightLog, params, new RowMapper<FlightContext>() {
                public FlightContext mapRow(ResultSet rs, int rowNum) throws SQLException {
                    StepResult stepResult;
                    if (rs.getBoolean("succeeded")) {
                        stepResult = StepResult.getStepResultSuccess();
                    } else {
                        stepResult = new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL,
                            getExceptionFromJson(rs.getString("exception")));
                    }

                    flightContext.getWorkingMap().fromJson(rs.getString("working_parameters"));

                    flightContext.setStepIndex(rs.getInt("step_index"));
                    flightContext.setDoing(rs.getBoolean("doing"));
                    flightContext.setResult(stepResult);
                    FlightStatus flightStatus = FlightStatus.valueOf(rs.getString("status"));
                    flightContext.setFlightStatus(flightStatus);
                    return flightContext;
                }
            });
            // There may not be any log entries for a given flight. That happens if we fail after
            // submit and before the first step. The defaults for flight context are correct for that
            // case, so there is nothing left to do here.
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
            " completed_time, output_parameters, status, exception, owner_id, owner_email" +
            " FROM " + FLIGHT_TABLE +
            " WHERE flightid = :flightid";

        MapSqlParameterSource params = new MapSqlParameterSource().addValue("flightid", flightId);

        try {
            return jdbcTemplate.queryForObject(sqlOneFlight, params, new FlightStateMapper());
        } catch (EmptyResultDataAccessException emptyEx) {
            throw new FlightNotFoundException("Flight not found: " + flightId, emptyEx);
        } catch (IncorrectResultSizeDataAccessException sizeEx) {
            throw new DatabaseOperationException("Multiple flights with the same id?! " + flightId, sizeEx);
        }
    }

    public List<FlightState> getFlights(int offset, int limit) {
        return getFlights(offset, limit, "", Collections.EMPTY_MAP);
    }

    public List<FlightState> getFlightsForUser(int offset, int limit, String subject) {
        return getFlights(offset, limit, " WHERE owner_id = :subject ", Collections.singletonMap("subject", subject));
    }

    private List<FlightState> getFlights(int offset, int limit, String whereClause, Map<String, ?> whereParams) {
        final String sqlFlightRange = "SELECT flightid, submit_time, input_parameters," +
            " completed_time, output_parameters, status, exception, owner_id, owner_email" +
            " FROM " + FLIGHT_TABLE +
            whereClause +
            " ORDER BY submit_time" +  // should this be descending?
            " LIMIT :limit OFFSET :offset";

        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("limit", limit)
            .addValue("offset", offset)
            .addValues(whereParams);

        List<FlightState> flightStateList = jdbcTemplate.query(sqlFlightRange, params, new FlightStateMapper());
        return flightStateList;
    }


    private static class FlightStateMapper implements RowMapper<FlightState> {
        public FlightState mapRow(ResultSet rs, int rowNum) throws SQLException {
            FlightState flightState = new FlightState();

            // Flight data that is always present
            flightState.setFlightId(rs.getString("flightid"));
            flightState.setFlightStatus(FlightStatus.valueOf(rs.getString("status")));
            flightState.setSubmitted(rs.getTimestamp("submit_time").toInstant());
            flightState.setUser(new UserRequestInfo()
                .subjectId(rs.getString("owner_id"))
                .name(rs.getString("owner_email")));

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
            return flightState;
        }
    }

    public String getExceptionJson(FlightContext flightContext) {
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
                inException instanceof PSQLException ||
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

    private static Exception getExceptionFromJson(String exceptionJson) {
        try {
            if (exceptionJson == null) {
                return null;
            }
            return getObjectMapper().readValue(exceptionJson, Exception.class);
        } catch (IOException ex) {
            throw new DatabaseOperationException("Failed to convert JSON to exception", ex);
        }
    }

    private static ObjectMapper getObjectMapper() {
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
}
