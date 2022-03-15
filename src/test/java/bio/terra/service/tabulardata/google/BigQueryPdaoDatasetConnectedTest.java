package bio.terra.service.tabulardata.google;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.buffer.model.ResourceInfo;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.PdaoConstant;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.exception.NotFoundException;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.model.TransactionModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.DatasetUtils;
import bio.terra.service.dataset.exception.TransactionLockException;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.resourcemanagement.BufferService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.google.GoogleResourceManagerService;
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

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticatedUserRequest.builder()
          .setSubjectId("DatasetUnit")
          .setEmail("dataset@unit.com")
          .setToken("token")
          .build();

  private static final AuthenticatedUserRequest TEST_USER_2 =
      AuthenticatedUserRequest.builder()
          .setSubjectId("DatasetUnit")
          .setEmail("dataset2@unit.com")
          .setToken("token2")
          .build();

  @Autowired private JsonLoader jsonLoader;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired private BigQueryPdao bigQueryPdao;
  @Autowired private DatasetDao datasetDao;
  @Autowired private SnapshotDao snapshotDao;
  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private ResourceService resourceService;
  @Autowired private GoogleResourceManagerService resourceManagerService;
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
        DatasetSummaryModel datasetSummaryModel = dataset.getDatasetSummary().toModel();
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

  @Test
  public void transactionTest() throws Exception {
    Dataset dataset = readDataset("ingest-test-dataset.json");
    String flightId1 = UUID.randomUUID().toString();
    String flightId2 = UUID.randomUUID().toString();
    String description1 = "foo";
    String description2 = null;
    String bucket = testConfig.getIngestbucket();
    String targetPath = "scratch/file" + UUID.randomUUID() + "/";

    BlobInfo participantBlob =
        BlobInfo.newBuilder(bucket, targetPath + "ingest-test-participant.json").build();
    storage.create(participantBlob, readFile("ingest-test-participant.json"));

    connectedOperations.addDataset(dataset.getId());
    bigQueryPdao.createDataset(dataset);

    // Create transactions
    TransactionModel transaction1 =
        bigQueryPdao.insertIntoTransactionTable(TEST_USER, dataset, flightId1, description1);
    TransactionModel transaction2 =
        bigQueryPdao.insertIntoTransactionTable(TEST_USER, dataset, flightId2, description2);

    assertThat("Transaction 1 has correct lock", transaction1.getLock(), is(flightId1));
    assertThat("Transaction 2 has correct lock", transaction2.getLock(), is(flightId2));

    assertThat(
        "Transaction 1 has correct description", transaction1.getDescription(), is(description1));
    assertThat(
        "Transaction 2 has correct description", transaction2.getDescription(), is(description2));

    assertThat(
        "Transaction 1 has correct status",
        transaction1.getStatus(),
        is(TransactionModel.StatusEnum.ACTIVE));
    assertThat(
        "Transaction 2 has correct status",
        transaction2.getStatus(),
        is(TransactionModel.StatusEnum.ACTIVE));

    // Enumerate all transactions
    List<TransactionModel> enumeratedTransactions =
        bigQueryPdao.enumerateTransactions(dataset, 0, 10);
    assertThat(
        "Enumerating transactions works",
        enumeratedTransactions,
        containsInAnyOrder(transaction1, transaction2));

    // Retrieve transactions individually
    assertThat(
        "Transaction 1 retrieves correctly",
        bigQueryPdao.retrieveTransaction(dataset, transaction1.getId()),
        is(transaction1));
    assertThat(
        "Transaction 2 retrieves correctly",
        bigQueryPdao.retrieveTransaction(dataset, transaction2.getId()),
        is(transaction2));

    // Lock transaction with same flight
    bigQueryPdao.updateTransactionTableLock(
        dataset, transaction1.getId(), transaction1.getLock(), TEST_USER);

    // Lock transaction with different flight
    assertThrows(
        TransactionLockException.class,
        () ->
            bigQueryPdao.updateTransactionTableLock(
                dataset, transaction1.getId(), "FOO", TEST_USER));

    // Ingest staged data into the new dataset.
    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .transactionId(transaction1.getId());

    UUID datasetId = dataset.getId();
    // Ingesting on a transaction that is locked already should fail
    connectedOperations.ingestTableFailure(
        datasetId,
        ingestRequest.table("participant").path(BigQueryPdaoTest.gsPath(participantBlob)),
        TEST_USER);

    // Unlock the first transaction so we can ingest
    bigQueryPdao.updateTransactionTableLock(dataset, transaction1.getId(), null, TEST_USER);

    // Ingesting on a transaction that was created by another user should fail
    connectedOperations.ingestTableFailure(
        datasetId,
        ingestRequest.table("participant").path(BigQueryPdaoTest.gsPath(participantBlob)),
        TEST_USER_2);

    // Finally!  An ingest that works
    connectedOperations.ingestTableSuccess(
        datasetId,
        ingestRequest.table("participant").path(BigQueryPdaoTest.gsPath(participantBlob)),
        TEST_USER);

    assertThat(
        "no rows are visible before commit",
        BigQueryProject.from(dataset)
            .query(
                String.format(
                    "SELECT * FROM `%s.%s.%s`",
                    dataset.getProjectResource().getGoogleProjectId(),
                    BigQueryPdao.prefixName(dataset.getName()),
                    "participant"))
            .getTotalRows(),
        equalTo(0L));

    // Commit the transaction
    bigQueryPdao.updateTransactionTableStatus(
        TEST_USER, dataset, transaction1.getId(), TransactionModel.StatusEnum.COMMITTED);

    assertThat(
        "rows are visible after commit",
        BigQueryProject.from(dataset)
            .query(
                String.format(
                    "SELECT * FROM `%s.%s.%s`",
                    dataset.getProjectResource().getGoogleProjectId(),
                    BigQueryPdao.prefixName(dataset.getName()),
                    "participant"))
            .getTotalRows(),
        equalTo(5L));

    // Try to reopen transaction
    assertThrows(
        IllegalArgumentException.class,
        () ->
            bigQueryPdao.updateTransactionTableStatus(
                TEST_USER, dataset, transaction1.getId(), TransactionModel.StatusEnum.ACTIVE));

    // Ingesting on a closed connection (transaction 1) should fail
    connectedOperations.ingestTableFailure(
        datasetId,
        ingestRequest.table("participant").path(BigQueryPdaoTest.gsPath(participantBlob)),
        TEST_USER);

    // Try to ingest in merge mode with transaction 2.  This should return conflict rows (need to
    // unlock transaction 2 first)
    bigQueryPdao.updateTransactionTableLock(dataset, transaction2.getId(), null, TEST_USER);
    connectedOperations.ingestTableSuccess(
        datasetId,
        ingestRequest
            .transactionId(transaction2.getId())
            .table("participant")
            .path(BigQueryPdaoTest.gsPath(participantBlob)),
        TEST_USER);

    assertThat(
        "Rows overlap",
        bigQueryPdao.verifyTransaction(
            dataset, dataset.getTableByName("participant").orElseThrow(), transaction2.getId()),
        equalTo(5L));

    // Delete the transactions
    bigQueryPdao.deleteFromTransactionTable(dataset, transaction1.getId());

    // Make sure we can't see the transaction
    assertThrows(
        NotFoundException.class,
        () -> bigQueryPdao.retrieveTransaction(dataset, transaction1.getId()));
    bigQueryPdao.deleteFromTransactionTable(dataset, transaction2.getId());
    assertThrows(
        NotFoundException.class,
        () -> bigQueryPdao.retrieveTransaction(dataset, transaction2.getId()));
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
    GoogleRegion region = GoogleRegion.fromValueWithDefault(datasetRequest.getRegion());
    Dataset dataset = DatasetUtils.convertRequestWithGeneratedNames(datasetRequest);
    dataset.id(UUID.randomUUID());
    ResourceInfo resource = bufferService.handoutResource(dataset.isSecureMonitoringEnabled());
    String googleProjectId = resource.getCloudResourceUid().getGoogleProjectUid().getProjectId();
    resourceManagerService.addLabelsToProject(
        googleProjectId, Map.of("test-name", "bigquery-pdao-test"));
    UUID projectId =
        resourceService.getOrCreateDatasetProject(
            profileModel, googleProjectId, region, dataset.getName(), dataset.getId());
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
