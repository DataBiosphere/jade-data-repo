package bio.terra.flight.dataset.create;

import bio.terra.category.Connected;
import bio.terra.dao.DrDatasetDao;
import bio.terra.fixtures.ConnectedOperations;
import bio.terra.metadata.DrDataset;
import bio.terra.model.DrDatasetJsonConversion;
import bio.terra.model.DrDatasetRequestModel;
import bio.terra.model.DrDatasetSummaryModel;
import bio.terra.pdao.PrimaryDataAccess;
import bio.terra.service.JobMapKeys;
import bio.terra.service.SamClientService;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.Stairway;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class DrDatasetCreateFlightTest {

    @Autowired
    private Stairway stairway;

    @Autowired
    private PrimaryDataAccess pdao;

    @Autowired
    private DrDatasetDao datasetDao;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private SamClientService samService;

    private String datasetName;
    private DrDatasetRequestModel datasetRequest;
    private DrDataset dataset;

    private DrDatasetRequestModel makeDatasetRequest(String datasetName) throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        ObjectReader reader = objectMapper.readerFor(DrDatasetRequestModel.class);
        InputStream stream = classLoader.getResourceAsStream("dataset-minimal.json");
        DrDatasetRequestModel datasetRequest = reader.readValue(stream);
        datasetRequest.setName(datasetName);
        return datasetRequest;
    }

    @Before
    public void setup() throws Exception {
        datasetName = "scftest" + StringUtils.remove(UUID.randomUUID().toString(), '-');
        datasetRequest = makeDatasetRequest(datasetName);
        dataset = DrDatasetJsonConversion.datasetRequestToDataset(datasetRequest);
        ConnectedOperations.stubOutSamCalls(samService);
    }

    @After
    public void tearDown() {
        if (pdao.datasetExists(datasetName)) {
            pdao.deleteDataset(dataset);
        }
        datasetDao.deleteByName(datasetName);
    }

    @Test
    public void testHappyPath() {
        FlightMap map = new FlightMap();
        map.put(JobMapKeys.REQUEST.getKeyName(), datasetRequest);
        String flightId = stairway.submit(DrDatasetCreateFlight.class, map);
        stairway.waitForFlight(flightId);

        FlightState result = stairway.getFlightState(flightId);
        assertEquals(FlightStatus.SUCCESS, result.getFlightStatus());
        Optional<FlightMap> resultMap = result.getResultMap();
        assertTrue(resultMap.isPresent());
        DrDatasetSummaryModel response = resultMap.get().get(JobMapKeys.RESPONSE.getKeyName(),
            DrDatasetSummaryModel.class);
        assertEquals(datasetName, response.getName());

        DrDataset createdDataset = datasetDao.retrieve(UUID.fromString(response.getId()));
        assertEquals(datasetName, createdDataset.getName());

        assertTrue(pdao.datasetExists(datasetName));
    }

    @Test
    public void testUndoAfterPrimaryDataStep() {
        FlightMap map = new FlightMap();
        map.put(JobMapKeys.REQUEST.getKeyName(), datasetRequest);
        String flightId = stairway.submit(UndoDrDatasetCreateFlight.class, map);
        stairway.waitForFlight(flightId);

        FlightState result = stairway.getFlightState(flightId);
        assertNotEquals(result.getFlightStatus(), FlightStatus.SUCCESS);
        Optional<Exception> errorMessage = result.getException();
        assertTrue(errorMessage.isPresent());
        assertThat(errorMessage.get().getMessage(), containsString("TestTriggerUndoStep"));

        boolean deletedSomething = datasetDao.deleteByName(datasetName);
        assertFalse(deletedSomething);

        assertFalse(pdao.datasetExists(datasetName));
    }
}
