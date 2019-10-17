package bio.terra.app.controller;

import bio.terra.category.Unit;
import bio.terra.common.fixtures.FlightStates;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.JobModel;
import bio.terra.service.iam.SamClientService;
import bio.terra.stairway.FlightState;
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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import static bio.terra.common.fixtures.DatasetFixtures.buildMinimalDatasetSummary;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("google")
@Category(Unit.class)
public class JobTest {

    @MockBean
    private Stairway stairway;

    @MockBean
    private SamClientService sam;

    @Autowired
    private MockMvc mvc;

    @Autowired
    private ObjectMapper objectMapper;

    private JobModel jobModel;

    private static final String testFlightId = FlightStates.testFlightId;
    private static final String submittedTimeFormatted = FlightStates.submittedTimeFormatted;
    private static final String completedTimeFormatted = FlightStates.completedTimeFormatted;


    @Before
    public void setup() {
        jobModel = new JobModel().id(testFlightId).description("This is not a job");
    }

    @Test
    public void retrieveJobStillRunningTest() throws Exception {
        FlightState flightState = FlightStates.makeFlightRunningState();

        when(stairway.getFlightState(any())).thenReturn(flightState);

        mvc.perform(get(String.format("/api/repository/v1/jobs/%s", testFlightId))
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(jobModel)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.id").value(testFlightId))
                .andExpect(jsonPath("$.description").value(buildMinimalDatasetSummary().getDescription()))
                .andExpect(jsonPath("$.job_status").value(JobModel.JobStatusEnum.RUNNING.toString()))
                .andExpect(jsonPath("$.submitted").value(submittedTimeFormatted))
                .andExpect(jsonPath("$.completed").isEmpty());
    }

    @Test
    public void retrieveJobTest() throws Exception {
        FlightState flightState = FlightStates.makeFlightCompletedState();

        when(stairway.getFlightState(any())).thenReturn(flightState);

        mvc.perform(get(String.format("/api/repository/v1/jobs/%s", testFlightId))
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(jobModel)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(testFlightId))
                .andExpect(jsonPath("$.description").value(buildMinimalDatasetSummary().getDescription()))
                .andExpect(jsonPath("$.job_status").value(JobModel.JobStatusEnum.SUCCEEDED.toString()))
                .andExpect(jsonPath("$.submitted").value(submittedTimeFormatted))
                .andExpect(jsonPath("$.completed").value(completedTimeFormatted));
    }

    @Test
    public void retrieveJobResultTest() throws Exception {
        FlightState flightState = FlightStates.makeFlightCompletedState();
        when(stairway.getFlightState(any())).thenReturn(flightState);

        DatasetSummaryModel req = buildMinimalDatasetSummary();

        mvc.perform(get(String.format("/api/repository/v1/jobs/%s/result", testFlightId))
            .header("Authorization", "Bearer: faketoken")
            .contentType(MediaType.APPLICATION_JSON)
            .content(objectMapper.writeValueAsString(req)))
            .andExpect(status().isIAmATeapot())
            .andExpect(jsonPath("$.id").value(req.getId()))
            .andExpect(jsonPath("$.name").value(req.getName()))
            .andExpect(jsonPath("$.description").value(req.getDescription()));
    }
}
