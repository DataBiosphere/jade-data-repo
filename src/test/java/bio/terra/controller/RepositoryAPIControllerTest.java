package bio.terra.controller;

import bio.terra.JobMapKeys;
import bio.terra.category.Unit;
import bio.terra.model.JobModel;
import bio.terra.model.StudySummaryModel;
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
import org.springframework.http.HttpStatus;
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
    private StudySummaryModel minimalStudySummary;

    private static final Timestamp submittedTime = Timestamp.from(Instant.now());
    private static final Timestamp completedTime = Timestamp.from(Instant.now());

    @Before
    public void setup() {
        jobModel = new JobModel().id(testFlightId).description("This is not a job");
        minimalStudySummary = new StudySummaryModel()
                .id("Minimal")
                .name("Minimal")
                .description("This is a sample study definition");
    }


    @Test
    public void enumerateJobsTest() throws Exception {
        FlightState flightState = makeFlightState();

        Integer offset = 0;
        Integer limit = 1;

        when(stairway.getFlights(eq(offset), eq(limit))).thenReturn(Arrays.asList(flightState));

        mvc.perform(get("/api/repository/v1/jobs?offset=0&limit=1")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Arrays.asList(jobModel))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[:1].id").value(testFlightId))
                .andExpect(jsonPath("$[:1].description").value(minimalStudySummary.getDescription()))
                .andExpect(jsonPath("$[:1].job_status").value(JobModel.JobStatusEnum.SUCCEEDED.toString()))
                .andExpect(jsonPath("$[:1].completed").exists());
    }

    @Test
    public void retrieveJobsTest() throws Exception {
        FlightState flightState = makeFlightState();

        when(stairway.getFlightState(any())).thenReturn(flightState);

        mvc.perform(get(String.format("/api/repository/v1/jobs/%s", testFlightId))
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(jobModel)))
                .andExpect(status().isSeeOther())
                .andExpect(jsonPath("$.id").value(testFlightId))
                .andExpect(jsonPath("$.description").value(minimalStudySummary.getDescription()))
                .andExpect(jsonPath("$.job_status").value(JobModel.JobStatusEnum.SUCCEEDED.toString()))
                .andExpect(jsonPath("$.completed").exists());
    }

    @Test
    public void retrieveJobResultTest() throws Exception {
        FlightState flightState = makeFlightState();
        when(stairway.getFlightState(any())).thenReturn(flightState);

        mvc.perform(get(String.format("/api/repository/v1/jobs/%s/result", testFlightId))
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(minimalStudySummary)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(minimalStudySummary.getId()))
                .andExpect(jsonPath("$.name").value(minimalStudySummary.getName()))
                .andExpect(jsonPath("$.description").value(minimalStudySummary.getDescription()));
    }

    private FlightState makeFlightState() {
        FlightMap resultMap = new FlightMap();
        resultMap.put(JobMapKeys.RESPONSE.getKeyName(), minimalStudySummary);
        resultMap.put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.OK);
        resultMap.put(JobMapKeys.DESCRIPTION.getKeyName(), minimalStudySummary.getDescription());

        FlightState flightState = new FlightState();
        flightState.setFlightId(testFlightId);
        flightState.setFlightStatus(FlightStatus.SUCCESS);
        flightState.setSubmitted(submittedTime);
        flightState.setInputParameters(resultMap);
        flightState.setResultMap(Optional.of(resultMap));
        flightState.setCompleted(Optional.of(completedTime));
        flightState.setErrorMessage(Optional.empty());
        return flightState;
    }
}
