package bio.terra.flight.dataset.create;

import bio.terra.category.Connected;
import bio.terra.service.dataset.flight.create.DatasetCreateFlight;
import bio.terra.stairway.UserRequestInfo;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.service.dataset.Dataset;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.DatasetJsonConversion;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.common.PrimaryDataAccess;
import bio.terra.service.resourcemanagement.ProfileDao;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.iam.SamClientService;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.Stairway;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
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
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class DatasetCreateFlightTest {

    @Autowired private Stairway stairway;
    @Autowired private PrimaryDataAccess pdao;
    @Autowired private DatasetDao datasetDao;
    @Autowired private ProfileDao profileDao;
    @Autowired private JsonLoader jsonLoader;
    @Autowired private GoogleResourceConfiguration googleResourceConfiguration;
    @Autowired private ConnectedOperations connectedOperations;

    @MockBean
    private SamClientService samService;

    private String datasetName;
    private DatasetRequestModel datasetRequest;
    private Dataset dataset;
    private BillingProfileModel billingProfileModel;
    private UserRequestInfo testUser =
        new UserRequestInfo().subjectId("StairwayUnit").name("stairway@unit.com");

    private DatasetRequestModel makeDatasetRequest(String datasetName, String profileId) throws IOException {
        DatasetRequestModel datasetRequest = jsonLoader.loadObject("dataset-minimal.json",
            DatasetRequestModel.class);
        return datasetRequest
            .name(datasetName)
            .defaultProfileId(profileId);
    }

    @Before
    public void setup() throws Exception {
        datasetName = "scftest" + StringUtils.remove(UUID.randomUUID().toString(), '-');
        billingProfileModel = connectedOperations.createProfileForAccount(
            googleResourceConfiguration.getCoreBillingAccount());
        datasetRequest = makeDatasetRequest(datasetName, billingProfileModel.getId());
        dataset = DatasetJsonConversion.datasetRequestToDataset(datasetRequest);
        connectedOperations.stubOutSamCalls(samService);
    }

    @After
    public void tearDown() {
        deleteDataset(dataset);
        datasetDao.deleteByName(datasetName);
        profileDao.deleteBillingProfileById(UUID.fromString(billingProfileModel.getId()));
    }

    /**
     * Fetches a dataset from the database based on it's name (handles the case where id isn't filled in)
     * @param dataset
     * @return
     */
    public boolean deleteDataset(Dataset dataset) {
        try {
            Dataset datasetFromDb = datasetDao.retrieveByName(dataset.getName());
            return pdao.deleteDataset(datasetFromDb);
        } catch (DatasetNotFoundException e) {
            return false;
        }
    }
    @Test
    public void testHappyPath() {
        FlightMap map = new FlightMap();
        map.put(JobMapKeys.REQUEST.getKeyName(), datasetRequest);
        String flightId = "successTest";
        stairway.submit(flightId, DatasetCreateFlight.class, map, testUser);
        stairway.waitForFlight(flightId);

        FlightState result = stairway.getFlightState(flightId);
        assertEquals(FlightStatus.SUCCESS, result.getFlightStatus());
        Optional<FlightMap> resultMap = result.getResultMap();
        assertTrue(resultMap.isPresent());
        DatasetSummaryModel response = resultMap.get().get(JobMapKeys.RESPONSE.getKeyName(), DatasetSummaryModel.class);
        assertEquals(datasetName, response.getName());

        Dataset createdDataset = datasetDao.retrieve(UUID.fromString(response.getId()));
        assertEquals(datasetName, createdDataset.getName());

        assertTrue(deleteDataset(dataset));
    }

    @Test
    public void testUndoAfterPrimaryDataStep() {
        FlightMap map = new FlightMap();
        map.put(JobMapKeys.REQUEST.getKeyName(), datasetRequest);
        String flightId = "undoTest";
        stairway.submit(flightId, UndoDatasetCreateFlight.class, map, testUser);
        stairway.waitForFlight(flightId);

        FlightState result = stairway.getFlightState(flightId);
        assertNotEquals(result.getFlightStatus(), FlightStatus.SUCCESS);
        Optional<Exception> errorMessage = result.getException();
        assertTrue(errorMessage.isPresent());
        assertThat(errorMessage.get().getMessage(), containsString("TestTriggerUndoStep"));

        boolean deletedSomething = datasetDao.deleteByName(datasetName);
        assertFalse(deletedSomething);

        assertFalse(deleteDataset(dataset));
    }
}
