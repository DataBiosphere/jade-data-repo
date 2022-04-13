package bio.terra.service.snapshot;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.auth.iam.IamProviderInterface;
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
public class SnapshotScaleConnectedTest {

  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private JsonLoader jsonLoader;
  @Autowired private MockMvc mvc;
  @Autowired private ObjectMapper objectMapper;
  @Autowired private ConfigurationService configService;
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
  public void snapshotRowIdsScaleTest() throws Exception {
    // use the dataset already created in setup

    // load add'l data rows into the dataset with rows from the GCS bucket
    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .table("thetable")
            .format(IngestRequestModel.FormatEnum.CSV)
            .path(
                "gs://jade-testdata/scratch/buildSnapshotWithRowIds/hca-mvp-analysis-file-row-ids-dataset-data.csv");

    ingestRequest.csvSkipLeadingRows(1);
    ingestRequest.csvGenerateRowIds(false);
    connectedOperations.ingestTableSuccess(datasetSummary.getId(), ingestRequest);

    // TODO put big snapshot request into a GCS bucket
    SnapshotRequestModel snapshotRequestScale =
        SnapshotConnectedTestUtils.makeSnapshotTestRequest(
            jsonLoader, datasetSummary, "hca-mvp-analysis-file-row-ids-snapshot.json");

    MockHttpServletResponse response =
        SnapshotConnectedTestUtils.performCreateSnapshot(
            connectedOperations, mvc, snapshotRequestScale, "");
    SnapshotSummaryModel summaryModel =
        SnapshotConnectedTestUtils.validateSnapshotCreated(
            connectedOperations, snapshotRequestScale, response);

    SnapshotModel snapshotModel =
        SnapshotConnectedTestUtils.getTestSnapshot(
            mvc, objectMapper, summaryModel.getId(), snapshotRequestScale, datasetSummary);

    connectedOperations.deleteTestSnapshot(snapshotModel.getId());
    // Duplicate delete should work
    connectedOperations.deleteTestSnapshot(snapshotModel.getId());
    connectedOperations.getSnapshotExpectError(snapshotModel.getId(), HttpStatus.NOT_FOUND);
  }
}
