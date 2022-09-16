package bio.terra.service.snapshot;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
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

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
@EmbeddedDatabaseTest
public class SnapshotHappyPathConnectedTest {

  @Autowired ConfigurationService configService;
  @Autowired private MockMvc mvc;
  @Autowired private ObjectMapper objectMapper;
  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private JsonLoader jsonLoader;
  @Autowired private ConnectedTestConfiguration testConfig;

  @MockBean private IamProviderInterface samService;

  private final Storage storage = StorageOptions.getDefaultInstance().getService();
  private DatasetSummaryModel datasetSummary;

  @Before
  public void setup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);
    configService.reset();
    BillingProfileModel billingProfile =
        connectedOperations.createProfileForAccount(testConfig.getGoogleBillingAccountId());
    datasetSummary =
        SnapshotConnectedTestUtils.createTestDataset(
            connectedOperations, billingProfile, "snapshot-test-dataset.json");
    SnapshotConnectedTestUtils.loadCsvData(
        connectedOperations,
        jsonLoader,
        storage,
        testConfig.getIngestbucket(),
        datasetSummary.getId(),
        "thetable",
        "snapshot-test-dataset-data-row-ids.csv");
  }

  @After
  public void tearDown() throws Exception {
    connectedOperations.teardown();
    configService.reset();
  }

  @Test
  public void testHappyPath() throws Exception {
    snapshotHappyPathTestingHelper("snapshot-test-snapshot.json");
  }

  @Test
  public void testFaultyPath() throws Exception {
    // Run the happy path test, but insert the GRANT ACCESS faults to simulate IAM propagation
    // failures
    configService.setFault(ConfigEnum.DATASET_GRANT_ACCESS_FAULT.name(), true);
    configService.setFault(ConfigEnum.SNAPSHOT_GRANT_ACCESS_FAULT.name(), true);
    testHappyPath();
  }

  @Test
  public void testRowIdsHappyPath() throws Exception {
    snapshotHappyPathTestingHelper("snapshot-row-ids-test-snapshot.json");
  }

  @Test
  public void testQueryHappyPath() throws Exception {
    snapshotHappyPathTestingHelper("snapshot-query-test-snapshot.json");
  }

  @Test
  public void testFullViewsHappyPath() throws Exception {
    snapshotHappyPathTestingHelper("snapshot-fullviews-test-snapshot.json");
  }

  private void snapshotHappyPathTestingHelper(String path) throws Exception {
    SnapshotRequestModel snapshotRequest =
        SnapshotConnectedTestUtils.makeSnapshotTestRequest(jsonLoader, datasetSummary, path, true);
    MockHttpServletResponse response =
        SnapshotConnectedTestUtils.performCreateSnapshot(
            connectedOperations, mvc, snapshotRequest, "_thp_");
    SnapshotSummaryModel summaryModel =
        SnapshotConnectedTestUtils.validateSnapshotCreated(
            connectedOperations, snapshotRequest, response);

    SnapshotModel snapshotModel =
        SnapshotConnectedTestUtils.getTestSnapshot(
            mvc, objectMapper, summaryModel.getId(), snapshotRequest, datasetSummary);

    connectedOperations.deleteTestSnapshot(snapshotModel.getId());
    // Duplicate delete should work
    connectedOperations.deleteTestSnapshot(snapshotModel.getId());
    connectedOperations.getSnapshotExpectError(snapshotModel.getId(), HttpStatus.NOT_FOUND);
  }
}
