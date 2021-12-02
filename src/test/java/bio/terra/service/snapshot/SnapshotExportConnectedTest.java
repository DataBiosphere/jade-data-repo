package bio.terra.service.snapshot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.iterableWithSize;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
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
public class SnapshotExportConnectedTest {

  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private MockMvc mvc;
  @Autowired private ObjectMapper objectMapper;
  @Autowired private JsonLoader jsonLoader;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired private ConfigurationService configService;
  @Autowired private ResourceService resourceService;
  @Autowired private SnapshotDao snapshotDao;
  @Autowired private BigQueryPdao bigQueryPdao;
  @Autowired private GcsPdao gcsPdao;

  @MockBean private IamProviderInterface samService;

  private final Storage storage = StorageOptions.getDefaultInstance().getService();
  private BillingProfileModel billingProfile;
  private Snapshot snapshot;

  @Before
  public void setup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);
    configService.reset();
    billingProfile =
        connectedOperations.createProfileForAccount(testConfig.getGoogleBillingAccountId());
    DatasetSummaryModel datasetMinimalSummary = setupMinimalDataset();

    SnapshotRequestModel snapshotRequest =
        SnapshotConnectedTestUtils.makeSnapshotTestRequest(
            jsonLoader, datasetMinimalSummary, "dataset-minimal-snapshot.json");
    MockHttpServletResponse response =
        SnapshotConnectedTestUtils.performCreateSnapshot(
            connectedOperations, mvc, snapshotRequest, "");
    SnapshotSummaryModel summaryModel =
        SnapshotConnectedTestUtils.validateSnapshotCreated(
            connectedOperations, snapshotRequest, response);
    snapshot = snapshotDao.retrieveSnapshot(summaryModel.getId());
  }

  @After
  public void tearDown() throws Exception {
    connectedOperations.teardown();
    configService.reset();
  }

  @Test
  public void testSnapshotExportToParquet() throws Exception {
    String flightId = "snapshotExportToParquet";
    GoogleBucketResource exportBucket =
        resourceService.getOrCreateBucketForSnapshotExport(snapshot, flightId);

    String bucketPath = String.format("gs://%s/%s/*", exportBucket.getName(), flightId);
    String participantPath =
        String.format("%s/participant/participant-000000000000.parquet", flightId);
    String samplePath = String.format("%s/sample/sample-000000000000.parquet", flightId);

    bigQueryPdao.exportTableToParquet(snapshot, exportBucket, flightId);

    List<BlobId> blobIds =
        gcsPdao.listGcsIngestBlobs(bucketPath, snapshot.getProjectResource().getGoogleProjectId());

    assertThat("Both tables got exported", blobIds, iterableWithSize(2));
    assertThat(
        "Export paths are named after tables",
        blobIds.stream().map(BlobId::getName).collect(Collectors.toList()),
        containsInAnyOrder(participantPath, samplePath));
  }

  private DatasetSummaryModel setupMinimalDataset() throws Exception {
    DatasetSummaryModel datasetMinimalSummary =
        SnapshotConnectedTestUtils.createTestDataset(
            connectedOperations, billingProfile, "dataset-minimal.json");
    SnapshotConnectedTestUtils.loadCsvData(
        connectedOperations,
        jsonLoader,
        storage,
        testConfig.getIngestbucket(),
        datasetMinimalSummary.getId(),
        "participant",
        "dataset-minimal-participant.csv");
    SnapshotConnectedTestUtils.loadCsvData(
        connectedOperations,
        jsonLoader,
        storage,
        testConfig.getIngestbucket(),
        datasetMinimalSummary.getId(),
        "sample",
        "dataset-minimal-sample.csv");
    return datasetMinimalSummary;
  }
}
