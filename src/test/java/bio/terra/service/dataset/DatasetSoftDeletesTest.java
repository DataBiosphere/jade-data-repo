package bio.terra.service.dataset;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.integration.BigQueryFixtures;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.TestJobWatcher;
import bio.terra.integration.UsersBase;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableResult;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

// TODO move me to integration dir
@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@AutoConfigureMockMvc
@Category(Integration.class)
public class DatasetSoftDeletesTest extends UsersBase {

  @Autowired private JsonLoader jsonLoader;
  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private AuthService authService;
  @Autowired private TestConfiguration testConfiguration;
  @Rule @Autowired public TestJobWatcher testWatcher;

  private String stewardToken;
  private UUID datasetId;
  private UUID profileId;
  private List<UUID> snapshotIds;

  @Before
  public void setup() throws Exception {
    super.setup();
    stewardToken = authService.getDirectAccessAuthToken(steward().getEmail());
    dataRepoFixtures.resetConfig(steward());
    profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
    datasetId = null;
    snapshotIds = new LinkedList<>();
  }

  @After
  public void teardown() throws Exception {
    dataRepoFixtures.resetConfig(steward());
    for (UUID snapshotId : snapshotIds) {
      dataRepoFixtures.deleteSnapshotLog(steward(), snapshotId);
    }

    if (datasetId != null) {
      dataRepoFixtures.deleteDatasetLog(steward(), datasetId);
    }

    if (profileId != null) {
      dataRepoFixtures.deleteProfileLog(steward(), profileId);
    }
  }

  @Test
  public void testSoftDeleteHappyPath() throws Exception {
    datasetId = ingestedDataset();

    // get row ids
    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
    BigQuery bigQuery = BigQueryFixtures.getBigQuery(dataset.getDataProject(), stewardToken);
    List<String> participantRowIds =
        DatasetIntegrationTest.getRowIds(bigQuery, dataset, "participant", 3L);
    List<String> sampleRowIds = DatasetIntegrationTest.getRowIds(bigQuery, dataset, "sample", 2L);

    // write them to GCS
    String participantPath =
        DatasetIntegrationTest.writeListToScratch(
            testConfiguration.getIngestbucket(), "softDel", participantRowIds);
    String samplePath =
        DatasetIntegrationTest.writeListToScratch(
            testConfiguration.getIngestbucket(), "softDel", sampleRowIds);

    // build the deletion request with pointers to the two files with row ids to soft delete
    List<DataDeletionTableModel> dataDeletionTableModels =
        Arrays.asList(
            DatasetIntegrationTest.deletionTableFile("participant", participantPath),
            DatasetIntegrationTest.deletionTableFile("sample", samplePath));
    DataDeletionRequest request =
        DatasetIntegrationTest.dataDeletionRequest().tables(dataDeletionTableModels);

    // send off the soft delete request
    dataRepoFixtures.deleteData(steward(), datasetId, request);

    // make sure the new counts make sense
    dataRepoFixtures.assertTableCount(steward(), dataset, "participant", 2);
    dataRepoFixtures.assertTableCount(steward(), dataset, "sample", 5);
  }

  @Test
  public void testSoftDeleteJsonArrayHappyPath() throws Exception {
    datasetId = ingestedDataset();

    // get row ids
    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
    BigQuery bigQuery = BigQueryFixtures.getBigQuery(dataset.getDataProject(), stewardToken);
    List<UUID> participantRowIds =
        DatasetIntegrationTest.getRowIds(bigQuery, dataset, "participant", 3L).stream()
            .map(UUID::fromString)
            .collect(Collectors.toList());
    List<UUID> sampleRowIds =
        DatasetIntegrationTest.getRowIds(bigQuery, dataset, "sample", 2L).stream()
            .map(UUID::fromString)
            .collect(Collectors.toList());

    // build the deletion request with pointers to the two files with row ids to soft delete
    List<DataDeletionTableModel> dataDeletionTableModels =
        Arrays.asList(
            DatasetIntegrationTest.deletionTableJson("participant", participantRowIds),
            DatasetIntegrationTest.deletionTableJson("sample", sampleRowIds));
    DataDeletionRequest request =
        DatasetIntegrationTest.dataDeletionRequest()
            .specType(DataDeletionRequest.SpecTypeEnum.JSONARRAY)
            .tables(dataDeletionTableModels);

    // send off the soft delete request
    dataRepoFixtures.deleteData(steward(), datasetId, request);

    // make sure the new counts make sense
    dataRepoFixtures.assertTableCount(steward(), dataset, "participant", 2);
    dataRepoFixtures.assertTableCount(steward(), dataset, "sample", 5);
  }

  @Test
  public void wildcardSoftDelete() throws Exception {
    datasetId = ingestedDataset();
    String pathPrefix = "softDelWildcard" + UUID.randomUUID();

    // get 5 row ids, we'll write them out to 5 separate files
    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
    BigQuery bigQuery = BigQueryFixtures.getBigQuery(dataset.getDataProject(), stewardToken);
    List<String> sampleRowIds = DatasetIntegrationTest.getRowIds(bigQuery, dataset, "sample", 5L);
    for (String rowId : sampleRowIds) {
      DatasetIntegrationTest.writeListToScratch(
          testConfiguration.getIngestbucket(), pathPrefix, Collections.singletonList(rowId));
    }

    // make a wildcard path 'gs://ingestbucket/softDelWildcard/*'
    String wildcardPath =
        String.format("gs://%s/scratch/%s/*", testConfiguration.getIngestbucket(), pathPrefix);

    // build a request and send it off
    DataDeletionRequest request =
        DatasetIntegrationTest.dataDeletionRequest()
            .tables(
                Collections.singletonList(
                    DatasetIntegrationTest.deletionTableFile("sample", wildcardPath)));
    dataRepoFixtures.deleteData(steward(), datasetId, request);

    // there should be (7 - 5) = 2 rows "visible" in the sample table
    dataRepoFixtures.assertTableCount(steward(), dataset, "sample", 2);
  }

  @Test
  public void testSoftDeleteNotInFullView() throws Exception {
    datasetId = ingestedDataset();

    // get row ids
    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
    BigQuery bigQuery = BigQueryFixtures.getBigQuery(dataset.getDataProject(), stewardToken);
    List<String> participantRowIds =
        DatasetIntegrationTest.getRowIds(bigQuery, dataset, "participant", 3L);
    List<String> sampleRowIds = DatasetIntegrationTest.getRowIds(bigQuery, dataset, "sample", 2L);

    // swap in these row ids in the request
    SnapshotRequestModel requestModelAll =
        jsonLoader.loadObject("ingest-test-snapshot-fullviews.json", SnapshotRequestModel.class);
    requestModelAll.getContents().get(0).datasetName(dataset.getName());

    SnapshotSummaryModel snapshotSummaryAll =
        dataRepoFixtures.createSnapshotWithRequest(
            steward(), dataset.getName(), profileId, requestModelAll);
    snapshotIds.add(snapshotSummaryAll.getId());
    SnapshotModel snapshotAll =
        dataRepoFixtures.getSnapshot(steward(), snapshotSummaryAll.getId(), null);
    // The steward is the custodian in this case, so is a reader in big query.
    BigQuery bigQueryAll = BigQueryFixtures.getBigQuery(snapshotAll.getDataProject(), stewardToken);

    assertSnapshotTableCount(bigQueryAll, snapshotAll, "participant", 5L);
    assertSnapshotTableCount(bigQueryAll, snapshotAll, "sample", 7L);

    // write them to GCS
    String participantPath =
        DatasetIntegrationTest.writeListToScratch(
            testConfiguration.getIngestbucket(), "softDel", participantRowIds);
    String samplePath =
        DatasetIntegrationTest.writeListToScratch(
            testConfiguration.getIngestbucket(), "softDel", sampleRowIds);

    // build the deletion request with pointers to the two files with row ids to soft delete
    List<DataDeletionTableModel> dataDeletionTableModels =
        Arrays.asList(
            DatasetIntegrationTest.deletionTableFile("participant", participantPath),
            DatasetIntegrationTest.deletionTableFile("sample", samplePath));
    DataDeletionRequest request =
        DatasetIntegrationTest.dataDeletionRequest().tables(dataDeletionTableModels);

    // send off the soft delete request
    dataRepoFixtures.deleteData(steward(), datasetId, request);

    // make sure the new counts make sense
    dataRepoFixtures.assertTableCount(steward(), dataset, "participant", 2);
    dataRepoFixtures.assertTableCount(steward(), dataset, "sample", 5);

    // make full views snapshot
    SnapshotRequestModel requestModelLess =
        jsonLoader.loadObject("ingest-test-snapshot-fullviews.json", SnapshotRequestModel.class);
    requestModelLess.getContents().get(0).datasetName(dataset.getName());

    SnapshotSummaryModel snapshotSummaryLess =
        dataRepoFixtures.createSnapshotWithRequest(
            steward(), dataset.getName(), profileId, requestModelLess);
    snapshotIds.add(snapshotSummaryLess.getId());

    SnapshotModel snapshotLess =
        dataRepoFixtures.getSnapshot(steward(), snapshotSummaryLess.getId(), null);
    BigQuery bigQueryLess =
        BigQueryFixtures.getBigQuery(snapshotLess.getDataProject(), stewardToken);

    // make sure the old counts stayed the same
    assertSnapshotTableCount(bigQueryAll, snapshotAll, "participant", 5L);
    assertSnapshotTableCount(bigQueryAll, snapshotAll, "sample", 7L);

    // make sure the new counts make sense
    assertSnapshotTableCount(bigQueryLess, snapshotLess, "participant", 2L);
    assertSnapshotTableCount(bigQueryLess, snapshotLess, "sample", 5L);
  }

  private void assertSnapshotTableCount(
      BigQuery bigQuery, SnapshotModel snapshot, String tableName, Long n)
      throws InterruptedException {

    String sql = "SELECT count(*) FROM " + BigQueryFixtures.makeTableRef(snapshot, tableName);
    TableResult result = BigQueryFixtures.queryWithRetry(sql, bigQuery);
    assertThat(
        "count matches", result.getValues().iterator().next().get(0).getLongValue(), equalTo(n));
  }

  private UUID ingestedDataset() throws Exception {
    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "ingest-test-dataset.json");
    UUID datasetId = datasetSummaryModel.getId();
    IngestRequestModel ingestRequest =
        dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-participant.json");
    IngestResponseModel ingestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
    assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(5L));

    ingestRequest =
        dataRepoFixtures.buildSimpleIngest("sample", "ingest-test/ingest-test-sample.json");
    ingestResponse = dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);
    assertThat("correct sample row count", ingestResponse.getRowCount(), equalTo(7L));
    return datasetId;
  }
}
