package bio.terra.controller;

import bio.terra.category.Unit;
import bio.terra.model.JobModel;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.Stairway;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class RepositoryAPIControllerTest {

    @MockBean
    private Stairway stairway;

    @Autowired
    private MockMvc mvc;

    @Autowired
    private ObjectMapper objectMapper;

    private JobModel jobModel;

    private static final String testFlightId = "test-flight-id";

    @Before
    public void setup() {
        jobModel = new JobModel().id(testFlightId).description("This is not a job");
    }


    @Test
    public void enumerateJobsTest() throws Exception {
        FlightState flightState = makeFlightState();

        int offset = 0;
        int limit = 1;

        when(stairway.getFlights(eq(offset), eq(limit))).thenReturn(Arrays.asList(flightState));

        mvc.perform(get("/api/repository/v1/jobs?offset=0&limit=1")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Arrays.asList(jobModel))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[:1].id").value(testFlightId));
        // TODO jobmodel has a description, but flights don't?
    }

    @Test
    public void retrieveJobsTest() throws Exception {
        FlightState flightState = makeFlightState();

        when(stairway.getFlightState(any())).thenReturn(flightState);

        mvc.perform(get(String.format("/api/repository/v1/jobs/%s", testFlightId))
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(jobModel)))
                .andExpect(status().isSeeOther())
                .andExpect(jsonPath("$.id").value(testFlightId));
        // TODO jobmodel has a description, but flights don't -- also why is this not returning Status?
    }

    @Test
    public void retrieveJobResultTest() throws Exception {
        FlightState flightState = makeFlightState();
        Object object = new Object();

        when(stairway.getFlightState(any())).thenReturn(flightState);

        mvc.perform(get(String.format("/api/repository/v1/jobs/%s/result", testFlightId))
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(object)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(testFlightId));
    }

    private FlightState makeFlightState() {
        Object object = new Object();
        // Construct a mock FlightState
        FlightMap resultMap = new FlightMap();
        resultMap.put("response", object);


        FlightState flightState = new FlightState();
        flightState.setFlightId(testFlightId);
        flightState.setFlightStatus(FlightStatus.SUCCESS);
        flightState.setSubmitted(Timestamp.from(Instant.now()));
        flightState.setInputParameters(resultMap); // unused
        flightState.setResultMap(Optional.of(resultMap));
        flightState.setCompleted(Optional.of(Timestamp.from(Instant.now())));
        flightState.setErrorMessage(Optional.empty());
        return flightState;
    }
}
