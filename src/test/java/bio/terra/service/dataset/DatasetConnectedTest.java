package bio.terra.service.dataset;

import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.ErrorModel;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.iam.IamService;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class DatasetConnectedTest {
    @Autowired private MockMvc mvc;
    @Autowired private JsonLoader jsonLoader;
    @Autowired private GoogleResourceConfiguration googleResourceConfiguration;
    @Autowired private ConnectedOperations connectedOperations;
    @Autowired private DatasetDao datasetDao;
    @Autowired private ConfigurationService configService;
    @MockBean private IamService samService;

    private BillingProfileModel billingProfile;

    @Before
    public void setup() throws Exception {
        connectedOperations.stubOutSamCalls(samService);
        billingProfile =
            connectedOperations.createProfileForAccount(googleResourceConfiguration.getCoreBillingAccount());
    }

    @After
    public void tearDown() throws Exception {
        connectedOperations.teardown();
    }

    @Test
    public void testDuplicateName() throws Exception {
        // create a dataset and check that it succeeds
        String resourcePath = "snapshot-test-dataset.json";
        DatasetRequestModel datasetRequest =
            jsonLoader.loadObject(resourcePath, DatasetRequestModel.class);
        datasetRequest
            .name(Names.randomizeName(datasetRequest.getName()))
            .defaultProfileId(billingProfile.getId());

        DatasetSummaryModel datasetSummaryModel = connectedOperations.createDataset(datasetRequest);
        assertNotNull("created dataset successfully the first time", datasetSummaryModel);

        // fetch the dataset and confirm the metadata matches the request
        DatasetModel datasetModel = connectedOperations.getDataset(datasetSummaryModel.getId());
        assertNotNull("fetched dataset successfully after creation", datasetModel);
        assertEquals("fetched dataset name matches request", datasetRequest.getName(), datasetModel.getName());

        // check that the dataset metadata row is unlocked
        DatasetSummary datasetSummary = datasetDao.retrieveSummaryByName(datasetRequest.getName());
        assertNull("dataset row is unlocked", datasetSummary.getFlightId());

        // try to create the same dataset again and check that it fails
        ErrorModel errorModel =
            connectedOperations.createDatasetExpectError(datasetRequest, HttpStatus.BAD_REQUEST);
        assertThat("error message includes name conflict",
            errorModel.getMessage(), containsString("Dataset name already exists"));

        // delete the dataset and check that it succeeds
        connectedOperations.deleteTestDataset(datasetSummaryModel.getId());

        // try to fetch the dataset again and confirm nothing is returned
        connectedOperations.getDatasetExpectError(datasetSummaryModel.getId(), HttpStatus.NOT_FOUND);
    }

    @Test
    public void testOverlappingDeletes() throws Exception {
        // create a dataset and check that it succeeds
        String resourcePath = "snapshot-test-dataset.json";
        DatasetRequestModel datasetRequest =
            jsonLoader.loadObject(resourcePath, DatasetRequestModel.class);
        datasetRequest
            .name(Names.randomizeName(datasetRequest.getName()))
            .defaultProfileId(billingProfile.getId());

        DatasetSummaryModel summaryModel = connectedOperations.createDataset(datasetRequest);

        // enable wait in DeleteDatasetPrimaryDataStep
        configService.setFault(ConfigEnum.DATASET_DELETE_LOCK_CONFLICT_STOP_FAULT.name(), true);

        // try to delete the dataset
        MvcResult result1 = mvc.perform(delete("/api/repository/v1/datasets/" + summaryModel.getId())).andReturn();

        // try to delete the dataset again
        MvcResult result2 = mvc.perform(delete("/api/repository/v1/datasets/" + summaryModel.getId())).andReturn();
        MockHttpServletResponse response2 = connectedOperations.validateJobModelAndWait(result2);
        ErrorModel errorModel2 = connectedOperations.handleFailureCase(response2, HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat("delete failed on lock exception", errorModel2.getMessage(),
            startsWith("Failed to lock the dataset"));

        // disable wait in DeleteDatasetPrimaryDataStep
        configService.setFault(ConfigEnum.DATASET_DELETE_LOCK_CONFLICT_CONTINUE_FAULT.name(), true);

        // check the response from the first delete request
        MockHttpServletResponse response1 = connectedOperations.validateJobModelAndWait(result1);
        DeleteResponseModel deleteResponseModel =
            connectedOperations.handleSuccessCase(response1, DeleteResponseModel.class);
        assertEquals("First delete returned successfully",
            DeleteResponseModel.ObjectStateEnum.DELETED, deleteResponseModel.getObjectState());

        // try to fetch the dataset again and confirm nothing is returned
        connectedOperations.getDatasetExpectError(summaryModel.getId(), HttpStatus.NOT_FOUND);
    }
}
