package bio.terra.controller;

import bio.terra.category.Unit;
import bio.terra.controller.exception.ApiException;
import bio.terra.dao.DrDatasetDao;
import bio.terra.dao.exception.DrDatasetNotFoundException;
import bio.terra.fixtures.FlightStates;
import bio.terra.fixtures.JsonLoader;
import bio.terra.flight.dataset.create.DrDatasetCreateFlight;
import bio.terra.metadata.DrDataset;
import bio.terra.model.DrDatasetJsonConversion;
import bio.terra.model.DrDatasetModel;
import bio.terra.model.DrDatasetRequestModel;
import bio.terra.service.SamClientService;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.Stairway;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.time.Instant;
import java.util.UUID;

import static bio.terra.fixtures.DrDatasetFixtures.buildDatasetRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class DrDatasetTest {

    @MockBean
    private Stairway stairway;

    @Autowired
    private MockMvc mvc;

    @MockBean
    private SamClientService sam;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JsonLoader jsonLoader;

    private static final String testFlightId = "test-flight-id";

    @Test
    public void testMinimalCreate() throws Exception {
        FlightState flightState = FlightStates.makeFlightSimpleState();

        when(stairway.submit(eq(DrDatasetCreateFlight.class), isA(FlightMap.class)))
                .thenReturn(testFlightId);
        // Call to mocked waitForFlight will do nothing, so no need to handle that
        when(stairway.getFlightState(eq(testFlightId)))
                .thenReturn(flightState);

        mvc.perform(post("/api/repository/v1/datasets")
            .contentType(MediaType.APPLICATION_JSON)
            .header("Authorization", "Bearer: faketoken")
            .content(objectMapper.writeValueAsString(buildDatasetRequest())))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.name").value("Minimal"))
            .andExpect(jsonPath("$.description")
                .value("This is a sample dataset definition"));
    }

    @Test
    public void testMinimalJsonCreate() throws Exception {
        FlightState flightState = FlightStates.makeFlightSimpleState();

        when(stairway.submit(eq(DrDatasetCreateFlight.class), isA(FlightMap.class)))
                .thenReturn(testFlightId);
        // Call to mocked waitForFlight will do nothing, so no need to handle that
        when(stairway.getFlightState(eq(testFlightId)))
                .thenReturn(flightState);

        String datasetJSON = jsonLoader.loadJson("dataset-minimal.json");
        mvc.perform(post("/api/repository/v1/datasets")
            .header("Authorization", "Bearer: faketoken")
            .contentType(MediaType.APPLICATION_JSON)
            .content(datasetJSON))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.name").value("Minimal"))
            .andExpect(jsonPath("$.description")
                .value("This is a sample dataset definition"));
    }

    @Test
    public void testFlightError() throws Exception {
        when(stairway.submit(eq(DrDatasetCreateFlight.class), isA(FlightMap.class)))
                .thenThrow(ApiException.class);
        mvc.perform(post("/api/repository/v1/datasets")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(buildDatasetRequest())))
                .andExpect(status().isInternalServerError());
    }

    @MockBean
    private DrDatasetDao datasetDao;


    @Test
    public void testDatasetRetrieve() throws Exception {
        assertThat("DrDataset retrieve with bad id gets 400",
                mvc.perform(get("/api/repository/v1/datasets/{id}", "blah"))
                        .andReturn().getResponse().getStatus(),
                equalTo(HttpStatus.BAD_REQUEST.value()));

        UUID missingId = UUID.fromString("cd100f94-e2c6-4d0c-aaf4-9be6651276a6");
        when(datasetDao.retrieve(eq(missingId))).thenThrow(
                new DrDatasetNotFoundException("DrDataset not found for id " + missingId.toString()));
        assertThat("DrDataset retrieve that doesn't exist returns 404",
                mvc.perform(get("/api/repository/v1/datasets/{id}", missingId))
                        .andReturn().getResponse().getStatus(),
                equalTo(HttpStatus.NOT_FOUND.value()));

        UUID id = UUID.fromString("8d2e052c-e1d1-4a29-88ed-26920907791f");
        DrDatasetRequestModel req = buildDatasetRequest();
        DrDataset dataset = DrDatasetJsonConversion.datasetRequestToDataset(req);
        dataset.id(id).createdDate(Instant.now());

        when(datasetDao.retrieve(eq(id))).thenReturn(dataset);
        assertThat("DrDataset retrieve returns 200",
                mvc.perform(get("/api/repository/v1/datasets/{id}", id.toString()))
                        .andReturn().getResponse().getStatus(),
                equalTo(HttpStatus.OK.value()));

        mvc.perform(get("/api/repository/v1/datasets/{id}", id.toString())).andDo((result) ->
                        assertThat("DrDataset retrieve returns a DrDataset Model with schema",
                                objectMapper.readValue(result.getResponse().getContentAsString(), DrDatasetModel.class)
                                        .getName(),
                                equalTo(req.getName())));

        assertThat("DrDataset retrieve returns a DrDataset Model with schema",
                objectMapper.readValue(
                        mvc.perform(get("/api/repository/v1/datasets/{id}", id))
                                .andReturn()
                                .getResponse()
                                .getContentAsString(),
                        DrDatasetModel.class).getName(),
                equalTo(req.getName()));
    }

}
