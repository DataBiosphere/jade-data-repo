package bio.terra.service.dataset;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.common.fixtures.JsonLoader;
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

  private UUID datasetId;
  private UUID profileId;
  private List<UUID> snapshotIds;

  @Before
  public void setup() throws Exception {
    super.setup();
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
    List<String> participantRowIds =
        dataRepoFixtures.getRowIds(steward(), dataset, "participant", 3);
    List<String> sampleRowIds = dataRepoFixtures.getRowIds(steward(), dataset, "sample", 2);

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
    dataRepoFixtures.assertDatasetTableCount(steward(), dataset, "participant", 2);
    dataRepoFixtures.assertDatasetTableCount(steward(), dataset, "sample", 5);
  }

  @Test
  public void testSoftDeleteJsonArrayHappyPath() throws Exception {
    datasetId = ingestedDataset();

    // get row ids
    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
    List<UUID> participantRowIds =
        dataRepoFixtures.getRowIds(steward(), dataset, "participant", 3).stream()
            .map(UUID::fromString)
            .collect(Collectors.toList());
    List<UUID> sampleRowIds =
        dataRepoFixtures.getRowIds(steward(), dataset, "sample", 2).stream()
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
    dataRepoFixtures.assertDatasetTableCount(steward(), dataset, "participant", 2);
    dataRepoFixtures.assertDatasetTableCount(steward(), dataset, "sample", 5);
  }

  @Test
  public void wildcardSoftDelete() throws Exception {
    datasetId = ingestedDataset();
    String pathPrefix = "softDelWildcard" + UUID.randomUUID();

    // get 5 row ids, we'll write them out to 5 separate files
    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
    List<String> sampleRowIds = dataRepoFixtures.getRowIds(steward(), dataset, "sample", 5);
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
    dataRepoFixtures.assertDatasetTableCount(steward(), dataset, "sample", 2);
  }

  @Test
  public void testSoftDeleteNotInFullView() throws Exception {
    datasetId = ingestedDataset();

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
    snapshotIds.add(snapshotSummaryAll.getId());
    SnapshotModel snapshotAll =
        dataRepoFixtures.getSnapshot(steward(), snapshotSummaryAll.getId(), null);
    dataRepoFixtures.assertSnapshotTableCount(steward(), snapshotAll, "participant", 5);
    dataRepoFixtures.assertSnapshotTableCount(steward(), snapshotAll, "sample", 7);

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
    dataRepoFixtures.assertDatasetTableCount(steward(), dataset, "participant", 2);
    dataRepoFixtures.assertDatasetTableCount(steward(), dataset, "sample", 5);

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

    // make sure the old counts stayed the same
    dataRepoFixtures.assertSnapshotTableCount(steward(), snapshotAll, "participant", 5);
    dataRepoFixtures.assertSnapshotTableCount(steward(), snapshotAll, "sample", 7);

    // make sure the new counts make sense
    dataRepoFixtures.assertSnapshotTableCount(steward(), snapshotLess, "participant", 2);
    dataRepoFixtures.assertSnapshotTableCount(steward(), snapshotLess, "sample", 5);
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
