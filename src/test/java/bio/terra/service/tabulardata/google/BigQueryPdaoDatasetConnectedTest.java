package bio.terra.service.tabulardata.google;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.buffer.model.ResourceInfo;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.PdaoConstant;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetJsonConversion;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.DatasetUtils;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.resourcemanagement.BufferService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import bio.terra.service.snapshot.SnapshotDao;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
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
import org.stringtemplate.v4.ST;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
@EmbeddedDatabaseTest
public class BigQueryPdaoDatasetConnectedTest {

  @Autowired private JsonLoader jsonLoader;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired private BigQueryPdao bigQueryPdao;
  @Autowired private DatasetDao datasetDao;
  @Autowired private SnapshotDao snapshotDao;
  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private ResourceService resourceService;
  @Autowired private GoogleProjectService projectService;
  @Autowired private BufferService bufferService;

  @MockBean private IamProviderInterface samService;

  private BillingProfileModel profileModel;

  private final Storage storage = StorageOptions.getDefaultInstance().getService();

  @Before
  public void setup() throws Exception {
    // Setup mock sam service
    connectedOperations.stubOutSamCalls(samService);

    String coreBillingAccount = testConfig.getGoogleBillingAccountId();
    profileModel = connectedOperations.createProfileForAccount(coreBillingAccount);
  }

  @After
  public void teardown() throws Exception {
    connectedOperations.teardown();
  }

  @Test
  public void datasetTest() throws Exception {
    Dataset defaultDataset = readDataset("ingest-test-dataset.json");
    Dataset eastDataset = readDataset("ingest-test-dataset-east.json");
    List<Object[]> testCases =
        Arrays.asList(
            new Object[] {
              defaultDataset,
              testConfig.getIngestbucket(),
              "BiqQuery datasets are instantiated in us-central1 by default."
            },
            new Object[] {
              eastDataset,
              testConfig.getNonDefaultRegionIngestBucket(),
              "BiqQuery datasets can be set to the non-default region."
            });

    for (Object[] tuple : testCases) {
      String targetPath = "scratch/file" + UUID.randomUUID().toString() + "/";
      Dataset dataset = (Dataset) tuple[0];
      String bucket = (String) tuple[1];
      String regionMessage = (String) tuple[2];

      String region =
          dataset
              .getDatasetSummary()
              .getStorageResourceRegion(GoogleCloudResource.BIGQUERY)
              .toString();

      connectedOperations.addDataset(dataset.getId());

      // Stage tabular data for ingest.
      BlobInfo participantBlob =
          BlobInfo.newBuilder(bucket, targetPath + "ingest-test-participant.json").build();
      BlobInfo sampleBlob =
          BlobInfo.newBuilder(bucket, targetPath + "ingest-test-sample.json").build();
      BlobInfo fileBlob = BlobInfo.newBuilder(bucket, targetPath + "ingest-test-file.json").build();

      BlobInfo missingPkBlob =
          BlobInfo.newBuilder(bucket, targetPath + "ingest-test-sample-no-id.json").build();
      BlobInfo nullPkBlob =
          BlobInfo.newBuilder(bucket, targetPath + "ingest-test-sample-null-id.json").build();

      try {
        bigQueryPdao.createDataset(dataset);

        com.google.cloud.bigquery.Dataset bqDataset = bigQueryDataset(dataset);
        assertThat(regionMessage, bqDataset.getLocation(), equalTo(region));

        storage.create(participantBlob, readFile("ingest-test-participant.json"));
        storage.create(sampleBlob, readFile("ingest-test-sample.json"));
        storage.create(fileBlob, readFile("ingest-test-file.json"));
        storage.create(missingPkBlob, readFile("ingest-test-sample-no-id.json"));
        storage.create(nullPkBlob, readFile("ingest-test-sample-null-id.json"));

        // Ingest staged data into the new dataset.
        IngestRequestModel ingestRequest =
            new IngestRequestModel().format(IngestRequestModel.FormatEnum.JSON);

        UUID datasetId = dataset.getId();
        connectedOperations.ingestTableSuccess(
            datasetId,
            ingestRequest.table("participant").path(BigQueryPdaoTest.gsPath(participantBlob)));
        connectedOperations.ingestTableSuccess(
            datasetId, ingestRequest.table("sample").path(BigQueryPdaoTest.gsPath(sampleBlob)));
        connectedOperations.ingestTableSuccess(
            datasetId, ingestRequest.table("file").path(BigQueryPdaoTest.gsPath(fileBlob)));

        // Check primary key non-nullability is enforced.
        connectedOperations.ingestTableFailure(
            datasetId, ingestRequest.table("sample").path(BigQueryPdaoTest.gsPath(missingPkBlob)));
        connectedOperations.ingestTableFailure(
            datasetId, ingestRequest.table("sample").path(BigQueryPdaoTest.gsPath(nullPkBlob)));

        // Create a snapshot!
        DatasetSummaryModel datasetSummaryModel =
            DatasetJsonConversion.datasetSummaryModelFromDatasetSummary(
                dataset.getDatasetSummary());
        SnapshotSummaryModel snapshotSummary =
            connectedOperations.createSnapshot(
                datasetSummaryModel, "ingest-test-snapshot.json", "");
        SnapshotModel snapshot = connectedOperations.getSnapshot(snapshotSummary.getId());

        com.google.cloud.bigquery.Dataset bqSnapshotDataset =
            BigQueryPdaoTest.bigQuerySnapshot(snapshot);

        assertThat(
            String.format(
                "Snapshot for dataset in region %s should also also be in region %s",
                region, region),
            region,
            equalTo(bqSnapshotDataset.getLocation()));

        BigQueryProject bigQueryProject =
            TestUtils.bigQueryProjectForDatasetName(datasetDao, dataset.getName());
        BigQueryProject bigQuerySnapshotProject =
            TestUtils.bigQueryProjectForSnapshotName(snapshotDao, snapshot.getName());
        assertThat(snapshot.getTables().size(), is(equalTo(3)));
        List<String> participantIds =
            BigQueryPdaoTest.queryForIds(
                snapshot.getName(), "participant", bigQuerySnapshotProject);
        List<String> sampleIds =
            BigQueryPdaoTest.queryForIds(snapshot.getName(), "sample", bigQuerySnapshotProject);
        List<String> fileIds =
            BigQueryPdaoTest.queryForIds(snapshot.getName(), "file", bigQuerySnapshotProject);

        assertThat(
            participantIds,
            containsInAnyOrder(
                "participant_1",
                "participant_2",
                "participant_3",
                "participant_4",
                "participant_5"));
        assertThat(sampleIds, containsInAnyOrder("sample1", "sample2", "sample5"));
        assertThat(fileIds, is(equalTo(Collections.singletonList("file1"))));

        // Simulate soft-deleting some rows.
        // TODO: Replace this with a call to the soft-delete API once it exists?
        softDeleteRows(
            bigQueryProject,
            bigQueryPdao.prefixName(dataset.getName()),
            BigQueryPdaoTest.getTable(dataset, "participant"),
            Arrays.asList("participant_3", "participant_4"));
        softDeleteRows(
            bigQueryProject,
            bigQueryPdao.prefixName(dataset.getName()),
            BigQueryPdaoTest.getTable(dataset, "sample"),
            Collections.singletonList("sample5"));
        softDeleteRows(
            bigQueryProject,
            bigQueryPdao.prefixName(dataset.getName()),
            BigQueryPdaoTest.getTable(dataset, "file"),
            Collections.singletonList("file1"));

        // Create another snapshot.
        snapshotSummary =
            connectedOperations.createSnapshot(
                datasetSummaryModel, "ingest-test-snapshot.json", "");
        SnapshotModel snapshot2 = connectedOperations.getSnapshot(snapshotSummary.getId());
        assertThat(snapshot2.getTables().size(), is(equalTo(3)));

        BigQueryProject bigQuerySnapshotProject2 =
            TestUtils.bigQueryProjectForSnapshotName(snapshotDao, snapshot2.getName());

        participantIds =
            BigQueryPdaoTest.queryForIds(
                snapshot2.getName(), "participant", bigQuerySnapshotProject2);
        sampleIds =
            BigQueryPdaoTest.queryForIds(snapshot2.getName(), "sample", bigQuerySnapshotProject2);
        fileIds =
            BigQueryPdaoTest.queryForIds(snapshot2.getName(), "file", bigQuerySnapshotProject2);
        assertThat(
            participantIds, containsInAnyOrder("participant_1", "participant_2", "participant_5"));
        assertThat(sampleIds, containsInAnyOrder("sample1", "sample2"));
        assertThat(fileIds, is(empty()));

        // Make sure the old snapshot wasn't changed.
        participantIds =
            BigQueryPdaoTest.queryForIds(
                snapshot.getName(), "participant", bigQuerySnapshotProject);
        sampleIds =
            BigQueryPdaoTest.queryForIds(snapshot.getName(), "sample", bigQuerySnapshotProject);
        fileIds = BigQueryPdaoTest.queryForIds(snapshot.getName(), "file", bigQuerySnapshotProject);
        assertThat(
            participantIds,
            containsInAnyOrder(
                "participant_1",
                "participant_2",
                "participant_3",
                "participant_4",
                "participant_5"));
        assertThat(sampleIds, containsInAnyOrder("sample1", "sample2", "sample5"));
        assertThat(fileIds, is(equalTo(Collections.singletonList("file1"))));
      } finally {
        storage.delete(
            participantBlob.getBlobId(),
            sampleBlob.getBlobId(),
            fileBlob.getBlobId(),
            missingPkBlob.getBlobId(),
            nullPkBlob.getBlobId());
      }
    }
  }

  private static final String queryAllRowIdsTemplate =
      "SELECT "
          + PdaoConstant.PDAO_ROW_ID_COLUMN
          + " FROM `<project>.<dataset>.<table>` "
          + "WHERE id IN UNNEST([<ids:{id|'<id>'}; separator=\",\">])";

  private void softDeleteRows(
      BigQueryProject bq, String datasetName, DatasetTable table, List<String> ids)
      throws Exception {

    ST sqlTemplate = new ST(queryAllRowIdsTemplate);
    sqlTemplate.add("project", bq.getProjectId());
    sqlTemplate.add("dataset", datasetName);
    sqlTemplate.add("table", table.getRawTableName());
    sqlTemplate.add("ids", ids);

    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(sqlTemplate.render())
            .setDestinationTable(TableId.of(datasetName, table.getSoftDeleteTableName()))
            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
            .build();

    bq.getBigQuery().query(queryConfig);
  }

  private byte[] readFile(String fileName) throws IOException {
    return IOUtils.toByteArray(getClass().getClassLoader().getResource(fileName));
  }

  private com.google.cloud.bigquery.Dataset bigQueryDataset(Dataset dataset) {
    return BigQueryProject.from(dataset)
        .getBigQuery()
        .getDataset(bigQueryPdao.prefixName(dataset.getName()));
  }

  // NOTE: This method bypasses the `connectedOperations` object, and creates a dataset
  // using lower-level method calls. This means that the dataset entry isn't auto-cleaned
  // as part of `connectedOperations.teardown()`. If you forget to manually delete any
  // datasets from the DAO at the end of a test, you'll see a FK violation when
  // `connectedOperations`
  // tries to delete the resource profile generated in `setup()`.
  //
  // This method is copied from BigQueryPdaoTest simply because the method contract would get huge
  // if the method were to be made static.
  private Dataset readDataset(String requestFile) throws Exception {
    String datasetName = BigQueryPdaoTest.makeDatasetName();
    DatasetRequestModel datasetRequest =
        jsonLoader.loadObject(requestFile, DatasetRequestModel.class);
    datasetRequest.defaultProfileId(profileModel.getId()).name(datasetName);
    GoogleRegion region =
        CloudPlatformWrapper.of(datasetRequest.getCloudPlatform())
            .getGoogleRegionFromDatasetRequestModel(datasetRequest);
    Dataset dataset = DatasetUtils.convertRequestWithGeneratedNames(datasetRequest);
    dataset.id(UUID.randomUUID());
    ResourceInfo resource = bufferService.handoutResource();
    String googleProjectId = resource.getCloudResourceUid().getGoogleProjectUid().getProjectId();
    projectService.addLabelsToProject(googleProjectId, Map.of("test-name", "bigquery-pdao-test"));
    UUID projectId =
        resourceService.getOrCreateDatasetProject(
            profileModel, googleProjectId, region, dataset.getName(), dataset.getId(), false);
    dataset
        .projectResourceId(projectId)
        .projectResource(resourceService.getProjectResource(projectId));

    String createFlightId = UUID.randomUUID().toString();
    UUID datasetId = UUID.randomUUID();
    dataset.id(datasetId);
    datasetDao.createAndLock(dataset, createFlightId);
    datasetDao.unlockExclusive(dataset.getId(), createFlightId);
    return dataset;
  }
}
