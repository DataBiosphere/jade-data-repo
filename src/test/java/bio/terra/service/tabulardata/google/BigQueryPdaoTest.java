package bio.terra.service.tabulardata.google;

import static bio.terra.common.PdaoConstant.PDAO_LOAD_HISTORY_STAGING_TABLE_PREFIX;
import static bio.terra.common.PdaoConstant.PDAO_LOAD_HISTORY_TABLE;
import static bio.terra.common.PdaoConstant.PDAO_PREFIX;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.app.model.GoogleRegion;
import bio.terra.buffer.model.ResourceInfo;
import bio.terra.common.Column;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.PdaoConstant;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BulkLoadFileState;
import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.model.SnapshotBuilderSettings;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.model.TableDataType;
import bio.terra.model.TransactionModel;
import bio.terra.model.TransactionModel.StatusEnum;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.DatasetUtils;
import bio.terra.service.resourcemanagement.BufferService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceManagerService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import bio.terra.service.snapshotbuilder.SnapshotBuilderSettingsDao;
import bio.terra.service.tabulardata.exception.BadExternalFileException;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDataResultModel;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import bio.terra.service.tabulardata.google.bigquery.BigQueryTransactionPdao;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public class BigQueryPdaoTest {
  private static final Logger logger = LoggerFactory.getLogger(BigQueryPdaoTest.class);

  @Autowired private JsonLoader jsonLoader;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired private BigQuerySnapshotPdao bigQuerySnapshotPdao;
  @Autowired private BigQueryDatasetPdao bigQueryDatasetPdao;
  @Autowired private BigQueryTransactionPdao bigQueryTransactionPdao;
  @Autowired private DatasetDao datasetDao;
  @Autowired private SnapshotDao snapshotDao;
  @Autowired private SnapshotBuilderSettingsDao settingsDao;
  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private ResourceService resourceService;
  @Autowired private GoogleResourceManagerService resourceManagerService;
  @Autowired private BufferService bufferService;
  @Autowired private SnapshotService snapshotService;
  @Autowired private SnapshotBuilderService snapshotBuilderService;

  @MockBean private IamProviderInterface samService;

  private BillingProfileModel profileModel;

  private final Storage storage = StorageOptions.getDefaultInstance().getService();

  private final List<UUID> datasetIdsToDelete = new ArrayList<>();
  private final List<Dataset> bqDatasetsToDelete = new ArrayList<>();

  private final List<BlobInfo> blobsToDelete = new ArrayList<>();

  @Rule public ExpectedException exceptionGrabber = ExpectedException.none();
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticatedUserRequest.builder()
          .setSubjectId("DatasetUnit")
          .setEmail("dataset@unit.com")
          .setToken("token")
          .build();

  @Before
  public void setup() throws Exception {
    // Setup mock sam service
    connectedOperations.stubOutSamCalls(samService);

    String coreBillingAccount = testConfig.getGoogleBillingAccountId();
    profileModel = connectedOperations.createProfileForAccount(coreBillingAccount);
  }

  @After
  public void teardown() throws Exception {
    blobsToDelete.forEach(blob -> storage.delete(blob.getBlobId()));
    connectedOperations.teardown();
  }

  private String insertExample =
      "INSERT INTO `broad-jade-dev.datarepo_hca_ebi.datarepo_load_history_staging_x` "
          + "(load_tag, load_time, source_name, target_path, state, file_id, checksum_crc32c, checksum_md5, error) "
          + "VALUES ('ebi_2020_08_15-0', '2020-08-16T01:27:54.733370Z', 'gs://broad-dsp-storage/blahblah.fastq.gz', "
          + "'/target/path', 'failed', '', '', '', \"\"\"bio.terra.common.exception.PdaoSourceFileNotFoundException: "
          + "Source file not found: 'gs://broad-dsp-storage/blahblah.fastq.gz'\"\"\")";

  @Test
  public void sanitizeErrorMsgTest() {
    List<BulkLoadHistoryModel> loadHistoryArray = new ArrayList<>();
    BulkLoadHistoryModel loadHistoryModel = new BulkLoadHistoryModel();
    loadHistoryModel.setSourcePath("gs://broad-dsp-storage/blahblah.fastq.gz");
    loadHistoryModel.setTargetPath("/target/path");
    loadHistoryModel.setState(BulkLoadFileState.FAILED);
    loadHistoryModel.setError(
        "bio.terra.common.exception.PdaoSourceFileNotFoundException: "
            + "Source file not found: 'gs://broad-dsp-storage/blahblah.fastq.gz'");
    loadHistoryArray.add(loadHistoryModel);

    ST sqlTemplate = new ST(bigQueryDatasetPdao.insertLoadHistoryToStagingTableTemplate);
    sqlTemplate.add("project", "broad-jade-dev");
    sqlTemplate.add("dataset", "datarepo_hca_ebi");
    sqlTemplate.add("stagingTable", PDAO_LOAD_HISTORY_STAGING_TABLE_PREFIX + "x");

    sqlTemplate.add("load_history_array", loadHistoryArray);
    sqlTemplate.add("load_tag", "ebi_2020_08_15-0");
    sqlTemplate.add("load_time", "2020-08-16T01:27:54.733370Z");

    assertEquals(insertExample, sqlTemplate.render());
  }

  @Test
  public void basicTest() throws Exception {
    Dataset dataset = readDataset("ingest-test-dataset.json");
    try {
      assertThatDatasetAndTablesShouldExist(dataset, false);

      bigQueryDatasetPdao.createDataset(dataset);
      assertThatDatasetAndTablesShouldExist(dataset, true);

      // Perform the redo, which should delete and re-create
      bigQueryDatasetPdao.createDataset(dataset);
      assertThatDatasetAndTablesShouldExist(dataset, true);

      // Now delete it and test that it is gone
      bigQueryDatasetPdao.deleteDataset(dataset);
      assertThatDatasetAndTablesShouldExist(dataset, false);
    } finally {
      datasetDao.delete(dataset.getId());
    }
  }

  @Test
  public void nonStringAssetRootTest() throws Exception {
    Dataset dataset = readDataset("ingest-test-dataset.json");

    // Stage tabular data for ingest.
    String targetPath = "scratch/file" + UUID.randomUUID() + "/";

    String bucket = testConfig.getIngestbucket();

    BlobInfo sampleBlob =
        BlobInfo.newBuilder(bucket, targetPath + "ingest-test-sample.json").build();
    BlobInfo fileBlob = BlobInfo.newBuilder(bucket, targetPath + "ingest-test-file.json").build();
    blobsToDelete.addAll(Arrays.asList(sampleBlob, fileBlob));

    bigQueryDatasetPdao.createDataset(dataset);

    storage.create(sampleBlob, readFile("ingest-test-sample.json"));

    // Ingest staged data into the new dataset.
    IngestRequestModel ingestRequest =
        new IngestRequestModel().format(IngestRequestModel.FormatEnum.JSON);

    UUID datasetId = dataset.getId();
    connectedOperations.ingestTableSuccess(
        datasetId, ingestRequest.table("sample").path(gsPath(sampleBlob)));

    // Create a snapshot!
    DatasetSummaryModel datasetSummaryModel = dataset.getDatasetSummary().toModel();
    SnapshotSummaryModel snapshotSummary =
        connectedOperations.createSnapshot(
            datasetSummaryModel, "ingest-test-snapshot-by-date.json", "");
    SnapshotModel snapshot = connectedOperations.getSnapshot(snapshotSummary.getId());

    BigQueryProject bigQuerySnapshotProject =
        TestUtils.bigQueryProjectForSnapshotName(snapshotDao, snapshot.getName());

    assertThat(snapshot.getTables().size(), is(equalTo(3)));
    List<String> sampleIds = queryForIds(snapshot.getName(), "sample", bigQuerySnapshotProject);

    assertThat(sampleIds, containsInAnyOrder("sample1", "sample2", "sample7"));
  }

  @Test
  public void partitionTest() throws Exception {
    Dataset dataset = readDataset("ingest-test-partitioned-dataset.json");
    String bqDatasetName = BigQueryPdao.prefixName(dataset.getName());

    try {
      bigQueryDatasetPdao.createDataset(dataset);
      BigQueryProject bigQueryProject =
          TestUtils.bigQueryProjectForDatasetName(datasetDao, dataset.getName());

      for (String tableName : Arrays.asList("participant", "sample", "file")) {
        DatasetTable table = getTable(dataset, tableName);

        ST queryTemplate = new ST(queryPartitionsSummaryTemplate);
        queryTemplate.add("project", bigQueryProject.getProjectId());
        queryTemplate.add("dataset", bqDatasetName);
        queryTemplate.add("table", table.getRawTableName());

        QueryJobConfiguration queryConfig =
            QueryJobConfiguration.newBuilder(queryTemplate.render())
                // Need legacy SQL to access the partitions summary meta-table.
                .setUseLegacySql(true)
                .build();

        // If the table isn't partitioned, this will go boom.
        bigQueryProject.getBigQuery().query(queryConfig);
      }

      // Make sure ingest date is exposed in live views, when it exists.
      ST queryTemplate = new ST(queryIngestDateTemplate);
      queryTemplate.add("project", bigQueryProject.getProjectId());
      queryTemplate.add("dataset", bqDatasetName);
      queryTemplate.add("table", "file");

      QueryJobConfiguration queryConfig = QueryJobConfiguration.of(queryTemplate.render());
      // If the column isn't exposed, this will go boom.
      bigQueryProject.getBigQuery().query(queryConfig);
    } finally {
      bigQueryDatasetPdao.deleteDataset(dataset);
      // Need to manually clean up the DAO because `readDataset` bypasses the
      // `connectedOperations` object, so we can't rely on its auto-teardown logic.
      datasetDao.delete(dataset.getId());
    }
  }

  private void ingestTable(
      Dataset dataset, String tableName, String ingestFile, int expectedRowCount) throws Exception {
    List<Map<String, Object>> data =
        jsonLoader.loadObjectAsStream(ingestFile, new TypeReference<>() {});
    var ingestRequestArray =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.ARRAY)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table(tableName)
            .records(List.of(data.toArray()));
    connectedOperations.ingestTableSuccess(dataset.getId(), ingestRequestArray);
    connectedOperations.checkTableRowCount(dataset, tableName, PDAO_PREFIX, expectedRowCount);
  }

  private Dataset stageOmopDataset() throws Exception {
    Dataset dataset = readDataset("omop/it-dataset-omop.json");
    SnapshotBuilderSettings settings = readSettings("omop/settings.json");
    connectedOperations.addDataset(dataset.getId());
    bigQueryDatasetPdao.createDataset(dataset);
    settingsDao.upsertSnapshotBuilderSettingsByDataset(dataset.getId(), settings);

    // Stage tabular data for ingest.
    ingestTable(dataset, "concept", "omop/concept-table-data.json", 7);
    ingestTable(dataset, "relationship", "omop/relationship.json", 2);
    ingestTable(dataset, "concept_ancestor", "omop/concept-ancestor-table-data.json", 10);
    ingestTable(dataset, "condition_occurrence", "omop/condition-occurrence-table-data.json", 53);
    ingestTable(dataset, "concept_relationship", "omop/concept-relationship-table-data.json", 4);

    return dataset;
  }

  private SnapshotBuilderSettings readSettings(String file) throws IOException {
    return jsonLoader.loadObject(file, SnapshotBuilderSettings.class);
  }

  @Test
  public void snapshotBuilderQuery() throws Exception {
    var dataset = stageOmopDataset();
    var conceptResponse = snapshotBuilderService.getConceptChildren(dataset.getId(), 2, TEST_USER);
    var concepts = conceptResponse.getResult();
    assertThat(concepts.size(), is(equalTo(2)));
    var concept1 = concepts.get(0);
    var concept3 = concepts.get(1);

    assertThat(concept1.getId(), is(equalTo(1)));
    assertThat(concept1.getCount(), is(equalTo(22)));
    assertThat(concept3.getId(), is(equalTo(3)));
    assertThat(concept3.getCount(), is(equalTo(24)));

    var searchConceptsResult =
        snapshotBuilderService.searchConcepts(dataset.getId(), 19, "concept1", TEST_USER);
    List<String> searchConceptNames =
        searchConceptsResult.getResult().stream().map(SnapshotBuilderConcept::getName).toList();
    assertThat(searchConceptNames, contains("concept1"));
  }

  @Test
  public void testGetFullViews() throws Exception {
    Dataset dataset = readDataset("ingest-test-dataset.json");
    connectedOperations.addDataset(dataset.getId());

    // Stage tabular data for ingest.
    String targetPath = "scratch/file" + UUID.randomUUID() + "/";

    String bucket = testConfig.getIngestbucket();

    BlobInfo participantBlob =
        BlobInfo.newBuilder(bucket, targetPath + "ingest-test-participant.json").build();
    BlobInfo sampleBlob =
        BlobInfo.newBuilder(bucket, targetPath + "ingest-test-sample.json").build();
    BlobInfo fileBlob = BlobInfo.newBuilder(bucket, targetPath + "ingest-test-file.json").build();

    try {
      bigQueryDatasetPdao.createDataset(dataset);

      storage.create(participantBlob, readFile("ingest-test-participant.json"));
      storage.create(sampleBlob, readFile("ingest-test-sample.json"));
      storage.create(fileBlob, readFile("ingest-test-file.json"));

      // Ingest staged data into the new dataset.
      IngestRequestModel ingestRequest =
          new IngestRequestModel().format(IngestRequestModel.FormatEnum.JSON);

      UUID datasetId = dataset.getId();
      // participant table
      String participantTableName = "participant";
      connectedOperations.ingestTableSuccess(
          datasetId, ingestRequest.table(participantTableName).path(gsPath(participantBlob)));
      connectedOperations.checkTableRowCount(dataset, participantTableName, PDAO_PREFIX, 5);
      connectedOperations.checkDataModel(
          dataset, List.of("id", "age"), PDAO_PREFIX, participantTableName, 5);
      // sample table
      String sampleTableName = "sample";
      connectedOperations.ingestTableSuccess(
          datasetId, ingestRequest.table(sampleTableName).path(gsPath(sampleBlob)));
      connectedOperations.checkTableRowCount(dataset, sampleTableName, PDAO_PREFIX, 7);
      // file table
      String fileTableName = "file";
      connectedOperations.ingestTableSuccess(
          datasetId, ingestRequest.table(fileTableName).path(gsPath(fileBlob)));
      connectedOperations.checkTableRowCount(dataset, fileTableName, PDAO_PREFIX, 1);

      // Create a full-view snapshot!
      DatasetSummaryModel datasetSummary = dataset.getDatasetSummary().toModel();
      SnapshotSummaryModel snapshotSummary =
          connectedOperations.createSnapshot(
              datasetSummary, "snapshot-fullviews-test-snapshot.json", "");
      Snapshot snapshot = snapshotService.retrieve(snapshotSummary.getId());
      connectedOperations.checkTableRowCount(snapshot, participantTableName, "", 5);
      connectedOperations.checkDataModel(
          snapshot, List.of("id", "age"), "", participantTableName, 5);
      connectedOperations.checkTableRowCount(snapshot, sampleTableName, "", 7);
      connectedOperations.checkTableRowCount(snapshot, fileTableName, "", 1);

      BigQueryProject bigQuerySnapshotProject =
          TestUtils.bigQueryProjectForSnapshotName(snapshotDao, snapshot.getName());

      assertThat(snapshot.getTables().size(), is(equalTo(3)));
      List<String> participantIds =
          queryForIds(snapshot.getName(), "participant", bigQuerySnapshotProject);
      List<String> sampleIds = queryForIds(snapshot.getName(), "sample", bigQuerySnapshotProject);
      List<String> fileIds = queryForIds(snapshot.getName(), "file", bigQuerySnapshotProject);

      assertThat(
          participantIds,
          containsInAnyOrder(
              "participant_1", "participant_2", "participant_3", "participant_4", "participant_5"));
      assertThat(
          sampleIds,
          containsInAnyOrder(
              "sample1", "sample2", "sample3", "sample4", "sample5", "sample6", "sample7"));
      assertThat(fileIds, is(equalTo(Collections.singletonList("file1"))));
    } finally {
      storage.delete(participantBlob.getBlobId(), sampleBlob.getBlobId(), fileBlob.getBlobId());
    }
  }

  @Test
  public void testBadSoftDeletePath() throws Exception {
    Dataset dataset = readDataset("ingest-test-dataset.json");
    String suffix = UUID.randomUUID().toString().replaceAll("-", "");
    String badGsUri =
        String.format("gs://%s/not/a/real/path/to/*files", testConfig.getIngestbucket());

    try {
      bigQueryDatasetPdao.createDataset(dataset);
      exceptionGrabber.expect(BadExternalFileException.class);
      bigQueryDatasetPdao.createSoftDeleteExternalTable(dataset, badGsUri, "participant", suffix);
    } finally {
      bigQueryDatasetPdao.deleteDataset(dataset);
      // Need to manually clean up the DAO because `readDataset` bypasses the
      // `connectedOperations` object, so we can't rely on its auto-teardown logic.
      datasetDao.delete(dataset.getId());
    }
  }

  private static final String snapshotTableDataSqlExample =
      "SELECT id, 'hello' as text" + " FROM UNNEST(GENERATE_ARRAY(1, 3)) AS id ORDER BY id;";

  @Test
  public void testGetSnapshotTableData() throws Exception {
    UUID profileId = profileModel.getId();
    ResourceInfo resourceInfo = bufferService.handoutResource(false);

    String dataProjectId = resourceInfo.getCloudResourceUid().getGoogleProjectUid().getProjectId();
    resourceManagerService.addLabelsToProject(
        dataProjectId, Map.of("test-name", "bigquery-pdao-test"));

    Snapshot snapshot =
        new Snapshot()
            .projectResource(
                new GoogleProjectResource().profileId(profileId).googleProjectId(dataProjectId));
    List<BigQueryDataResultModel> expected = getExampleSnapshotTableData();
    List<BigQueryDataResultModel> actual =
        bigQuerySnapshotPdao.getSnapshotTableUnsafe(snapshot, snapshotTableDataSqlExample);
    for (int i = 0; i < 3; i++) {
      assertEquals(expected.get(i).getRowResult(), actual.get(i).getRowResult());
    }
  }

  @Test
  public void testFileIdUpdateMethods() throws Exception {
    DatasetRequestModel datasetRequest =
        jsonLoader.loadObject("ingest-test-dataset.json", DatasetRequestModel.class);
    datasetRequest.getSchema().getTables().stream()
        .filter(t -> t.getName().equals("file"))
        .findFirst()
        .map(
            t ->
                t.addColumnsItem(
                    new ColumnModel().name("file").datatype(TableDataType.FILEREF).arrayOf(false)))
        .map(
            t ->
                t.addColumnsItem(
                    new ColumnModel()
                        .name("file_a")
                        .datatype(TableDataType.FILEREF)
                        .arrayOf(true)));

    Dataset dataset = readDataset(datasetRequest);

    // Stage tabular data for ingest.
    String targetPath = "scratch/file" + UUID.randomUUID() + "/";
    String bucket = testConfig.getIngestbucket();
    BlobInfo fileBlob = BlobInfo.newBuilder(bucket, targetPath + "ingest-test-file.json").build();

    try {
      assertThatDatasetAndTablesShouldExist(dataset, false);

      bigQueryDatasetPdao.createDataset(dataset);
      assertThatDatasetAndTablesShouldExist(dataset, true);

      //  ingest data into the new dataset.
      UUID newFileId1 = UUID.randomUUID();
      UUID newFileId2 = UUID.randomUUID();
      String fileIngestDataTmpl1 =
          "{\"sourcePath\":\"%s\", \"targetPath\":\"/file/%s\"}".formatted(gsPath(fileBlob), "0");
      String fileIngestDataTmpl2 =
          "{\"sourcePath\":\"%s\", \"targetPath\":\"/file/%s\"}".formatted(gsPath(fileBlob), "1");
      String ingestData =
          """
        {"id":"file1","derived_from":["sample6","sample2"],"file":%s,"file_a":[%s]}
      """
              .formatted(fileIngestDataTmpl1, fileIngestDataTmpl2);
      storage.create(fileBlob, ingestData.getBytes(StandardCharsets.UTF_8));
      IngestRequestModel ingestRequest =
          new IngestRequestModel().format(IngestRequestModel.FormatEnum.JSON);
      connectedOperations.ingestTableSuccess(
          dataset.getId(), ingestRequest.table("file").path(gsPath(fileBlob)));
      List<String> originalIds = readIds(dataset);

      // Insert the Id mappings
      // Note: we need a transaction to power the live view used as part of inserting new file ids
      String flightId = "myflight";
      TransactionModel transaction =
          bigQueryTransactionPdao.insertIntoTransactionTable(TEST_USER, dataset, flightId, null);
      bigQueryDatasetPdao.createStagingFileIdMappingTable(dataset);
      bigQueryDatasetPdao.fileIdMappingToStagingTable(
          dataset,
          Map.of(
              UUID.fromString(originalIds.get(0)), newFileId1,
              UUID.fromString(originalIds.get(1)), newFileId2));

      // Update the metadata table
      bigQueryDatasetPdao.insertNewFileIdsIntoDatasetTable(
          dataset, getTable(dataset, "file"), transaction.getId(), flightId, TEST_USER);
      // Commit the transaction
      bigQueryTransactionPdao.updateTransactionTableStatus(
          TEST_USER, dataset, transaction.getId(), StatusEnum.COMMITTED);

      List<String> newIds = readIds(dataset);
      assertThat(
          "new file ids are now stored",
          newIds,
          containsInAnyOrder(newFileId1.toString(), newFileId2.toString()));

      // Now delete it and test that it is gone
      bigQueryDatasetPdao.deleteDataset(dataset);
      assertThatDatasetAndTablesShouldExist(dataset, false);
    } finally {
      datasetDao.delete(dataset.getId());
    }
  }

  private List<String> readIds(Dataset dataset) throws Exception {
    BigQueryProject bigQueryDatasetProject =
        TestUtils.bigQueryProjectForDatasetName(datasetDao, dataset.getName());
    List<String> ids =
        new ArrayList<>(
            queryForColumn(
                BigQueryPdao.prefixName(dataset.getName()),
                getTable(dataset, "file").getName(),
                new Column().name("file"),
                bigQueryDatasetProject));
    List<List<String>> idsFromArray =
        queryForColumn(
            BigQueryPdao.prefixName(dataset.getName()),
            getTable(dataset, "file").getName(),
            new Column().name("file_a").arrayOf(true),
            bigQueryDatasetProject);
    ids.addAll(idsFromArray.stream().flatMap(Collection::stream).toList());
    return ids;
  }

  private List<BigQueryDataResultModel> getExampleSnapshotTableData() {
    List<BigQueryDataResultModel> values = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      values.add(
          new BigQueryDataResultModel()
              .rowResult(Map.of("id", String.valueOf(i + 1), "text", "hello"))
              .filteredCount(3)
              .totalCount(3));
    }
    return values;
  }

  static com.google.cloud.bigquery.Dataset bigQuerySnapshot(SnapshotModel snapshot) {
    return BigQueryProject.from(snapshot).getBigQuery().getDataset(snapshot.getName());
  }

  private byte[] readFile(String fileName) throws IOException {
    return IOUtils.toByteArray(getClass().getClassLoader().getResource(fileName));
  }

  static String gsPath(BlobInfo blob) {
    return "gs://" + blob.getBucket() + "/" + blob.getName();
  }

  private static final String queryForColumnTemplate =
      "SELECT <column> FROM `<project>.<container>.<table>` ORDER BY id";

  // Query for a table / column's value.
  static <T> List<T> queryForColumn(
      String containerName, String tableName, Column column, BigQueryProject bigQueryProject)
      throws Exception {
    String bigQueryProjectId = bigQueryProject.getProjectId();
    BigQuery bigQuery = bigQueryProject.getBigQuery();

    ST sqlTemplate = new ST(queryForColumnTemplate);
    sqlTemplate.add("project", bigQueryProjectId);
    sqlTemplate.add("container", containerName);
    sqlTemplate.add("table", tableName);
    sqlTemplate.add("column", column.getName());

    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(sqlTemplate.render()).build();
    TableResult result = bigQuery.query(queryConfig);

    ArrayList<T> ids = new ArrayList<>();
    if (column.isArrayOf()) {
      result
          .iterateAll()
          .forEach(
              r ->
                  ids.add(
                      (T)
                          r.get(column.getName()).getRepeatedValue().stream()
                              .map(v -> v.getValue())
                              .toList()));
    } else {
      result.iterateAll().forEach(r -> ids.add((T) r.get(column.getName()).getValue()));
    }

    return ids;
  }

  static List<String> queryForIds(
      String snapshotName, String tableName, BigQueryProject bigQueryProject) throws Exception {
    return queryForColumn(snapshotName, tableName, new Column().name("id"), bigQueryProject);
  }

  /* BigQuery Legacy SQL supports querying a "meta-table" about partitions
   * for any partitioned table.
   *
   * The table won't exist if the real table is unpartitioned, so we can
   * query it for a quick check to see if we enabled the expected options
   * on table creation.
   *
   * https://cloud.google.com/bigquery/docs/
   *   creating-partitioned-tables#listing_partitions_in_ingestion-time_partitioned_tables
   */
  private static final String queryPartitionsSummaryTemplate =
      "SELECT * FROM [<project>.<dataset>.<table>$__PARTITIONS_SUMMARY__]";

  private static final String queryIngestDateTemplate =
      "SELECT "
          + PdaoConstant.PDAO_INGEST_DATE_COLUMN_ALIAS
          + " FROM `<project>.<dataset>.<table>`";

  static String makeDatasetName() {
    return "pdaotest" + StringUtils.remove(UUID.randomUUID().toString(), '-');
  }

  // NOTE: This method bypasses the `connectedOperations` object, and creates a dataset
  // using lower-level method calls. This means that the dataset entry isn't auto-cleaned
  // as part of `connectedOperations.teardown()`. If you forget to manually delete any
  // datasets from the DAO at the end of a test, you'll see a FK violation when
  // `connectedOperations`
  // tries to delete the resource profile generated in `setup()`.
  private Dataset readDataset(String requestFile) throws Exception {
    DatasetRequestModel datasetRequest =
        jsonLoader.loadObject(requestFile, DatasetRequestModel.class);
    return readDataset(datasetRequest);
  }

  private Dataset readDataset(DatasetRequestModel datasetRequest) throws Exception {
    String datasetName = makeDatasetName();
    datasetRequest.defaultProfileId(profileModel.getId()).name(datasetName);
    GoogleRegion region = GoogleRegion.fromValueWithDefault(datasetRequest.getRegion());
    Dataset dataset = DatasetUtils.convertRequestWithGeneratedNames(datasetRequest);
    dataset.id(UUID.randomUUID());
    ResourceInfo resource = bufferService.handoutResource(false);
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
    connectedOperations.addDataset(dataset.getId());
    return dataset;
  }

  static DatasetTable getTable(Dataset dataset, String name) {
    return dataset
        .getTableByName(name)
        .orElseThrow(() -> new IllegalStateException("Expected table " + name + " not found!"));
  }

  private void assertThatDatasetAndTablesShouldExist(Dataset dataset, boolean shouldExist)
      throws InterruptedException {

    boolean datasetExists = bigQueryDatasetPdao.tableExists(dataset, "participant");
    assertThat(
        String.format("Dataset: %s, exists", dataset.getName()),
        datasetExists,
        equalTo(shouldExist));

    boolean loadTableExists = bigQueryDatasetPdao.tableExists(dataset, PDAO_LOAD_HISTORY_TABLE);
    assertThat(
        String.format("Load Table: %s, exists", PDAO_LOAD_HISTORY_TABLE),
        loadTableExists,
        equalTo(shouldExist));

    for (String name : Arrays.asList("participant", "sample", "file")) {
      DatasetTable table = getTable(dataset, name);
      for (String t :
          Arrays.asList(table.getName(), table.getRawTableName(), table.getSoftDeleteTableName())) {
        assertThat(
            "Table: " + dataset.getName() + "." + t + ", exists",
            bigQueryDatasetPdao.tableExists(dataset, t),
            equalTo(shouldExist));
      }
    }
  }
}
