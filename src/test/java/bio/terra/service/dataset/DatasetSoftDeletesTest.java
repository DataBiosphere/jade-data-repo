package bio.terra.service.dataset;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.IntegrationTestConfiguration;
import bio.terra.integration.Users;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

// TODO move me to integration dir
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = IntegrationTestConfiguration.class)
@ActiveProfiles({"google", "integrationtest"})
@Tag(Integration.TAG)
@Execution(ExecutionMode.CONCURRENT)
class DatasetSoftDeletesTest {

  @Autowired private JsonLoader jsonLoader;
  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private TestConfiguration testConfiguration;
  @Autowired private Users users;

  private final ThreadLocal<TestConfiguration.User> steward =
      ThreadLocal.withInitial(() -> users.steward());
  private final ThreadLocal<UUID> tlDatasetId = new ThreadLocal<>();
  private final ThreadLocal<UUID> tlProfileId = new ThreadLocal<>();
  private final ThreadLocal<List<UUID>> snapshotIds = ThreadLocal.withInitial(ArrayList::new);

  private TestConfiguration.User steward() {
    return steward.get();
  }

  @BeforeEach
  public void setup() throws Exception {
    dataRepoFixtures.resetConfig(steward());
    tlProfileId.set(dataRepoFixtures.createBillingProfile(steward()).getId());
    ingestDataset();
  }

  @AfterEach
  public void teardown() throws Exception {
    dataRepoFixtures.resetConfig(steward());
    for (UUID snapshotId : snapshotIds.get()) {
      dataRepoFixtures.deleteSnapshotLog(steward(), snapshotId);
    }

    var datasetId = tlDatasetId.get();
    if (datasetId != null) {
      dataRepoFixtures.deleteDatasetLog(steward(), datasetId);
    }

    var profileId = tlProfileId.get();
    if (profileId != null) {
      dataRepoFixtures.deleteProfileLog(steward(), profileId);
    }
  }

  @Test
  void testSoftDeleteHappyPath() throws Exception {
    var datasetId = tlDatasetId.get();
    // get row ids
    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
    List<String> participantRowIds =
        dataRepoFixtures.getRowIds(steward(), dataset, "participant", 3);
    List<String> sampleRowIds = dataRepoFixtures.getRowIds(steward(), dataset, "sample", 2);

    // write them to GCS
    String participantPath =
        DatasetIntegrationTest.writeListToScratch(
            testConfiguration.ingestbucket(), "softDel", participantRowIds);
    String samplePath =
        DatasetIntegrationTest.writeListToScratch(
            testConfiguration.ingestbucket(), "softDel", sampleRowIds);

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
    dataRepoFixtures.assertDatasetTableCount(steward(), dataset, "participant", 2);
    dataRepoFixtures.assertDatasetTableCount(steward(), dataset, "sample", 5);
  }

  @Test
  void testSoftDeleteJsonArrayHappyPath() throws Exception {
    var datasetId = tlDatasetId.get();
    // get row ids
    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
    List<UUID> participantRowIds =
        dataRepoFixtures.getRowIds(steward(), dataset, "participant", 3).stream()
            .map(UUID::fromString)
            .toList();
    List<UUID> sampleRowIds =
        dataRepoFixtures.getRowIds(steward(), dataset, "sample", 2).stream()
            .map(UUID::fromString)
            .toList();

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
    dataRepoFixtures.assertDatasetTableCount(steward(), dataset, "participant", 2);
    dataRepoFixtures.assertDatasetTableCount(steward(), dataset, "sample", 5);
  }

  @Test
  void wildcardSoftDelete() throws Exception {
    var datasetId = tlDatasetId.get();
    String pathPrefix = "softDelWildcard" + UUID.randomUUID();

    // get 5 row ids, we'll write them out to 5 separate files
    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
    List<String> sampleRowIds = dataRepoFixtures.getRowIds(steward(), dataset, "sample", 5);
    for (String rowId : sampleRowIds) {
      DatasetIntegrationTest.writeListToScratch(
          testConfiguration.ingestbucket(), pathPrefix, Collections.singletonList(rowId));
    }

    // make a wildcard path 'gs://ingestbucket/softDelWildcard/*'
    String wildcardPath =
        String.format("gs://%s/scratch/%s/*", testConfiguration.ingestbucket(), pathPrefix);

    // build a request and send it off
    DataDeletionRequest request =
        DatasetIntegrationTest.dataDeletionRequest()
            .tables(
                Collections.singletonList(
                    DatasetIntegrationTest.deletionTableFile("sample", wildcardPath)));
    dataRepoFixtures.deleteData(steward(), datasetId, request);

    // there should be (7 - 5) = 2 rows "visible" in the sample table
    dataRepoFixtures.assertDatasetTableCount(steward(), dataset, "sample", 2);
  }

  @Test
  void testSoftDeleteNotInFullView() throws Exception {
    var datasetId = tlDatasetId.get();
    var profileId = tlProfileId.get();
    // get row ids
    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
    List<String> participantRowIds =
        dataRepoFixtures.getRowIds(steward(), dataset, "participant", 3);
    List<String> sampleRowIds = dataRepoFixtures.getRowIds(steward(), dataset, "sample", 2);

    // swap in these row ids in the request
    SnapshotRequestModel requestModelAll =
        jsonLoader.loadObject("ingest-test-snapshot-fullviews.json", SnapshotRequestModel.class);
    requestModelAll.getContents().get(0).datasetName(dataset.getName());

    SnapshotSummaryModel snapshotSummaryAll =
        dataRepoFixtures.createSnapshotWithRequest(
            steward(), dataset.getName(), profileId, requestModelAll);
    snapshotIds.get().add(snapshotSummaryAll.getId());
    SnapshotModel snapshotAll =
        dataRepoFixtures.getSnapshot(steward(), snapshotSummaryAll.getId(), null);
    dataRepoFixtures.assertSnapshotTableCount(steward(), snapshotAll, "participant", 5);
    dataRepoFixtures.assertSnapshotTableCount(steward(), snapshotAll, "sample", 7);

    // write them to GCS
    String participantPath =
        DatasetIntegrationTest.writeListToScratch(
            testConfiguration.ingestbucket(), "softDel", participantRowIds);
    String samplePath =
        DatasetIntegrationTest.writeListToScratch(
            testConfiguration.ingestbucket(), "softDel", sampleRowIds);

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
    dataRepoFixtures.assertDatasetTableCount(steward(), dataset, "participant", 2);
    dataRepoFixtures.assertDatasetTableCount(steward(), dataset, "sample", 5);

    // make full views snapshot
    SnapshotRequestModel requestModelLess =
        jsonLoader.loadObject("ingest-test-snapshot-fullviews.json", SnapshotRequestModel.class);
    requestModelLess.getContents().get(0).datasetName(dataset.getName());

    SnapshotSummaryModel snapshotSummaryLess =
        dataRepoFixtures.createSnapshotWithRequest(
            steward(), dataset.getName(), profileId, requestModelLess);
    snapshotIds.get().add(snapshotSummaryLess.getId());

    SnapshotModel snapshotLess =
        dataRepoFixtures.getSnapshot(steward(), snapshotSummaryLess.getId(), null);

    // make sure the old counts stayed the same
    dataRepoFixtures.assertSnapshotTableCount(steward(), snapshotAll, "participant", 5);
    dataRepoFixtures.assertSnapshotTableCount(steward(), snapshotAll, "sample", 7);

    // make sure the new counts make sense
    dataRepoFixtures.assertSnapshotTableCount(steward(), snapshotLess, "participant", 2);
    dataRepoFixtures.assertSnapshotTableCount(steward(), snapshotLess, "sample", 5);
  }

  private void ingestDataset() throws Exception {
    var profileId = tlProfileId.get();
    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "ingest-test-dataset.json");
    var datasetId = datasetSummaryModel.getId();
    tlDatasetId.set(datasetId);
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
  }
}
