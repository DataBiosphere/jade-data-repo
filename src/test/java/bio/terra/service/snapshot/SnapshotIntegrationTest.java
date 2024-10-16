package bio.terra.service.snapshot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import bio.terra.common.PdaoConstant;
import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.integration.DataRepoClient;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.UsersBase;
import bio.terra.model.DatasetDataModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.PolicyModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotPreviewModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestModelPolicies;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "integrationtest"})
@Category(Integration.class)
public class SnapshotIntegrationTest extends UsersBase {
  @Autowired private DataRepoClient dataRepoClient;

  @Autowired private JsonLoader jsonLoader;

  @Autowired private DataRepoFixtures dataRepoFixtures;

  @Autowired private AuthService authService;

  private static final Logger logger = LoggerFactory.getLogger(SnapshotIntegrationTest.class);
  private UUID profileId;
  private DatasetSummaryModel datasetSummaryModel;
  private UUID datasetId;
  private final List<UUID> createdSnapshotIds = new ArrayList<>();
  private String stewardToken;
  String participantTableName;
  int participantTableRowCount;

  @Before
  public void setup() throws Exception {
    super.setup();
    stewardToken = authService.getDirectAccessAuthToken(steward().getEmail());
    profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
    dataRepoFixtures.addPolicyMember(
        steward(), profileId, IamRole.USER, custodian().getEmail(), IamResourceType.SPEND_PROFILE);

    datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "ingest-test-dataset.json");
    datasetId = datasetSummaryModel.getId();
    dataRepoFixtures.addDatasetPolicyMember(
        steward(), datasetId, IamRole.CUSTODIAN, custodian().getEmail());

    IngestRequestModel request =
        dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-participant.json");
    participantTableName = "participant";
    participantTableRowCount = 5;
    dataRepoFixtures.ingestJsonData(steward(), datasetId, request);
    request = dataRepoFixtures.buildSimpleIngest("sample", "ingest-test/ingest-test-sample.json");
    dataRepoFixtures.ingestJsonData(steward(), datasetId, request);
  }

  @After
  public void tearDown() throws Exception {
    createdSnapshotIds.forEach(
        snapshot -> {
          try {
            dataRepoFixtures.deleteSnapshot(steward(), snapshot);
          } catch (Exception ex) {
            logger.warn("cleanup failed when deleting snapshot " + snapshot);
            ex.printStackTrace();
          }
        });

    if (datasetId != null) {
      dataRepoFixtures.deleteDatasetLog(steward(), datasetId);
    }

    if (profileId != null) {
      dataRepoFixtures.deleteProfileLog(steward(), profileId);
    }
  }

  @Test
  public void snapshotRowIdsHappyPathTest() throws Exception {
    // fetch rowIds from the ingested dataset by querying the participant table
    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
    String participantTable = "participant";
    String sampleTable = "sample";

    List<Object> participantResults =
        dataRepoFixtures
            .retrieveDatasetData(steward(), datasetId, participantTable, 0, 1000, null)
            .getResult();
    List<UUID> participantIds =
        participantResults.stream()
            .map(
                r ->
                    UUID.fromString(
                        ((LinkedHashMap) r).get(PdaoConstant.PDAO_ROW_ID_COLUMN).toString()))
            .toList();
    List<Object> sampleResults =
        dataRepoFixtures
            .retrieveDatasetData(steward(), datasetId, sampleTable, 0, 1000, null)
            .getResult();
    List<UUID> sampleIds =
        sampleResults.stream()
            .map(
                r ->
                    UUID.fromString(
                        ((LinkedHashMap) r).get(PdaoConstant.PDAO_ROW_ID_COLUMN).toString()))
            .toList();

    // swap in these row ids in the request
    SnapshotRequestModel requestModel =
        jsonLoader.loadObject("ingest-test-snapshot-row-ids-test.json", SnapshotRequestModel.class);
    requestModel.getContents().get(0).getRowIdSpec().getTables().get(0).setRowIds(participantIds);
    requestModel.getContents().get(0).getRowIdSpec().getTables().get(1).setRowIds(sampleIds);

    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshotWithRequest(
            steward(), dataset.getName(), profileId, requestModel);
    TimeUnit.SECONDS.sleep(10);
    createdSnapshotIds.add(snapshotSummary.getId());
    SnapshotModel snapshot = dataRepoFixtures.getSnapshot(steward(), snapshotSummary.getId(), null);
    assertEquals("new snapshot has been created", snapshot.getName(), requestModel.getName());
    assertEquals(
        "new snapshot has the correct number of tables",
        requestModel.getContents().get(0).getRowIdSpec().getTables().size(),
        snapshot.getTables().size());
    // TODO: get the snapshot and make sure the number of rows matches with the row ids input
    assertThat(
        "The secure monitoring is propagated from the dataset",
        snapshot.getSource().get(0).getDataset().isSecureMonitoringEnabled(),
        is(false));

    assertThat(
        "The phs ID is propagated from the dataset",
        snapshot.getSource().get(0).getDataset().getPhsId(),
        equalTo("phs100321"));

    assertThat(
        "The phs ID is set in snapshot summary", snapshotSummary.getPhsId(), equalTo("phs100321"));

    assertThat("The consent code is set in the snapshot", snapshot.getConsentCode(), equalTo("c1"));

    assertThat(
        "The consent code is set in the snapshot summary",
        snapshotSummary.getConsentCode(),
        equalTo("c1"));

    List<String> stewardRoles =
        dataRepoFixtures.retrieveUserSnapshotRoles(steward(), snapshotSummary.getId());
    assertThat("The Steward was given steward access", stewardRoles, hasItem("steward"));
  }

  @Test
  public void snapshotByQueryHappyPathTest() throws Exception {
    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
    SnapshotRequestModel requestModel = snapshotByQueryRequestModel(dataset);
    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshotWithRequest(
            steward(), dataset.getName(), profileId, requestModel);
    TimeUnit.SECONDS.sleep(10);
    createdSnapshotIds.add(snapshotSummary.getId());
    SnapshotModel snapshot = dataRepoFixtures.getSnapshot(steward(), snapshotSummary.getId(), null);
    assertEquals("new snapshot has been created", snapshot.getName(), requestModel.getName());
  }

  @Test
  public void snapshotByAssetHappyPathTest() throws Exception {
    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
    String datasetName = dataset.getName();
    SnapshotRequestModel requestModel =
        jsonLoader.loadObject("ingest-test-snapshot-asset.json", SnapshotRequestModel.class);
    // swap in the correct dataset name (with the id at the end)
    requestModel.getContents().get(0).setDatasetName(datasetName);
    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshotWithRequest(
            steward(), dataset.getName(), profileId, requestModel);
    TimeUnit.SECONDS.sleep(10);
    createdSnapshotIds.add(snapshotSummary.getId());
    SnapshotModel snapshot = dataRepoFixtures.getSnapshot(steward(), snapshotSummary.getId(), null);
    assertEquals("new snapshot has been created", snapshot.getName(), requestModel.getName());
  }

  @Test
  public void deleteAssetWithSnapshotTest() throws Exception {
    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
    SnapshotRequestModel requestModel = snapshotByQueryRequestModel(dataset);
    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshotWithRequest(
            steward(), dataset.getName(), profileId, requestModel);
    TimeUnit.SECONDS.sleep(10);
    ErrorModel errorModel =
        dataRepoFixtures.deleteDatasetAssetExpectFailure(steward(), datasetId, "sample_centric");
    assertThat(
        "Error deleting asset",
        errorModel.getMessage(),
        containsString("The asset is being used by snapshots: " + snapshotSummary.getId()));
    dataRepoFixtures.deleteSnapshot(steward(), snapshotSummary.getId());
    dataRepoFixtures.deleteDatasetAsset(steward(), datasetId, "sample_centric");
  }

  @Test
  public void retrieveRowCountAndSnapshotByFullViewTest() throws Exception {
    // DATASET
    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
    String datasetName = dataset.getName();

    // Empty dataset table
    dataRepoFixtures.assertDatasetTableCount(steward(), dataset, "file", 0);

    // Non-empty dataset table, no filtering: total row count = filtered row count > 0
    dataRepoFixtures.assertDatasetTableCount(
        steward(), dataset, participantTableName, participantTableRowCount);

    // Non-empty dataset table, filtered results: total row count > filtered row count > 0
    DatasetDataModel filteredDatasetDataModel =
        dataRepoFixtures.retrieveDatasetData(
            steward(),
            dataset.getId(),
            participantTableName,
            0,
            participantTableRowCount + 1,
            "WHERE id = 'participant_1'");
    int expectedFilteredRowCount = 1;
    assertThat(
        "With no limit, number of results should equal the filtered row count",
        filteredDatasetDataModel.getResult().size(),
        equalTo(expectedFilteredRowCount));
    assertThat(
        "Total row count matches expected total row count",
        filteredDatasetDataModel.getTotalRowCount(),
        equalTo(participantTableRowCount));
    assertThat(
        "Filtered row count matches expected filtered row count",
        filteredDatasetDataModel.getFilteredRowCount(),
        equalTo(expectedFilteredRowCount));

    // Non-empty dataset table, filtered results to 0 rows: total row count > filtered row count = 0
    DatasetDataModel emptyFilteredDatasetDataModel =
        dataRepoFixtures.retrieveDatasetData(
            steward(),
            dataset.getId(),
            participantTableName,
            0,
            participantTableRowCount + 1,
            "WHERE (id = 'invalid')");
    expectedFilteredRowCount = 0;
    assertThat(
        "With no limit, number of results should equal the filtered row count",
        emptyFilteredDatasetDataModel.getResult().size(),
        equalTo(expectedFilteredRowCount));
    assertThat(
        "Total row count matches expected total row count",
        emptyFilteredDatasetDataModel.getTotalRowCount(),
        equalTo(participantTableRowCount));
    assertThat(
        "Filtered row count matches expected filtered row count",
        emptyFilteredDatasetDataModel.getFilteredRowCount(),
        equalTo(expectedFilteredRowCount));

    // SNAPSHOT
    // create snapshot by full view
    SnapshotRequestModel requestModel =
        jsonLoader.loadObject("ingest-test-snapshot-fullviews.json", SnapshotRequestModel.class);
    requestModel.getContents().get(0).setDatasetName(datasetName);
    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshotWithRequest(steward(), datasetName, profileId, requestModel);
    TimeUnit.SECONDS.sleep(10);
    createdSnapshotIds.add(snapshotSummary.getId());
    SnapshotModel snapshot = dataRepoFixtures.getSnapshot(steward(), snapshotSummary.getId(), null);
    assertEquals("new snapshot has been created", snapshot.getName(), requestModel.getName());
    assertEquals("the relationship comes through", 1, snapshot.getRelationships().size());

    // Empty snapshot table
    dataRepoFixtures.assertSnapshotTableCount(steward(), snapshot, "file", 0);

    // Non-empty snapshot table, no filtering: total row count = filtered row count > 0
    dataRepoFixtures.assertSnapshotTableCount(
        steward(), snapshot, participantTableName, participantTableRowCount);

    // Non-empty snapshot table, filtered results: total row count > filtered row count > 0
    SnapshotPreviewModel filteredSnapshotPreviewModel =
        dataRepoFixtures.retrieveSnapshotPreviewById(
            steward(),
            snapshot.getId(),
            participantTableName,
            0,
            participantTableRowCount + 1,
            "id = 'participant_1'");
    expectedFilteredRowCount = 1;
    assertThat(
        "With no limit, number of results should equal the filtered row count",
        filteredSnapshotPreviewModel.getResult().size(),
        equalTo(expectedFilteredRowCount));
    assertThat(
        "Total row count matches expected total row count",
        filteredSnapshotPreviewModel.getTotalRowCount(),
        equalTo(participantTableRowCount));
    assertThat(
        "Filtered row count matches expected filtered row count",
        filteredSnapshotPreviewModel.getFilteredRowCount(),
        equalTo(expectedFilteredRowCount));

    // Non-empty snapshot table, filtered results to 0 rows: total row count > filtered row count =
    // 0
    SnapshotPreviewModel emptyFilteredSnapshotPreviewModel =
        dataRepoFixtures.retrieveSnapshotPreviewById(
            steward(),
            snapshot.getId(),
            participantTableName,
            0,
            participantTableRowCount + 1,
            "id = 'invalid'");
    expectedFilteredRowCount = 0;
    assertThat(
        "With no limit, number of results should equal the filtered row count",
        emptyFilteredSnapshotPreviewModel.getResult().size(),
        equalTo(expectedFilteredRowCount));
    assertThat(
        "Total row count matches expected total row count",
        emptyFilteredSnapshotPreviewModel.getTotalRowCount(),
        equalTo(participantTableRowCount));
    assertThat(
        "Filtered row count matches expected filtered row count",
        emptyFilteredSnapshotPreviewModel.getFilteredRowCount(),
        equalTo(expectedFilteredRowCount));
  }

  @Test
  public void snapshotByFullViewAndPetServiceAccountHappyPathTest() throws Exception {
    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
    String datasetName = dataset.getName();
    SnapshotRequestModel requestModel =
        jsonLoader.loadObject("ingest-test-snapshot-fullviews.json", SnapshotRequestModel.class);
    // swap in the correct dataset name (with the id at the end)
    requestModel.getContents().get(0).setDatasetName(datasetName);
    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshotWithRequest(
            steward(), datasetName, profileId, requestModel, true, true);
    TimeUnit.SECONDS.sleep(10);
    createdSnapshotIds.add(snapshotSummary.getId());
    SnapshotModel snapshot = dataRepoFixtures.getSnapshot(steward(), snapshotSummary.getId(), null);
    assertEquals("new snapshot has been created", snapshot.getName(), requestModel.getName());
    assertEquals("the relationship comes through", 1, snapshot.getRelationships().size());
  }

  private SnapshotRequestModel snapshotByQueryRequestModel(DatasetModel dataset) throws Exception {
    String datasetName = dataset.getName();
    SnapshotRequestModel requestModel =
        jsonLoader.loadObject("ingest-test-snapshot-query.json", SnapshotRequestModel.class);
    // swap in the correct dataset name (with the id at the end)
    requestModel.getContents().get(0).setDatasetName(datasetName);
    requestModel
        .getContents()
        .get(0)
        .getQuerySpec()
        .setQuery(
            "SELECT "
                + datasetName
                + ".sample.datarepo_row_id FROM "
                + datasetName
                + ".sample WHERE "
                + datasetName
                + ".sample.id ='sample6'");
    return requestModel;
  }

  @Test
  public void testCreateSnapshotWithPolicies() throws Exception {
    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
    String datasetName = dataset.getName();
    SnapshotRequestModel requestModel =
        jsonLoader.loadObject("ingest-test-snapshot-fullviews.json", SnapshotRequestModel.class);
    // swap in the correct dataset name (with the id at the end)
    requestModel.getContents().get(0).setDatasetName(datasetName);

    List<String> stewards = List.of(steward().getEmail(), admin().getEmail());
    String readerEmail = reader().getEmail();
    List<String> readersWithDuplicates = List.of(readerEmail, readerEmail);
    String discovererEmail = discoverer().getEmail();
    SnapshotRequestModelPolicies policiesRequest =
        new SnapshotRequestModelPolicies()
            .stewards(stewards)
            .readers(readersWithDuplicates)
            .addDiscoverersItem(discovererEmail);
    requestModel.setPolicies(policiesRequest);

    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshotWithRequest(steward(), datasetName, profileId, requestModel);
    TimeUnit.SECONDS.sleep(10);
    UUID snapshotId = snapshotSummary.getId();
    createdSnapshotIds.add(snapshotId);

    Map<String, List<String>> rolesToPolicies =
        dataRepoFixtures.retrieveSnapshotPolicies(steward(), snapshotId).getPolicies().stream()
            .collect(Collectors.toMap(PolicyModel::getName, PolicyModel::getMembers));

    assertThat(
        "All specified stewards added on snapshot creation",
        rolesToPolicies.get(IamRole.STEWARD.toString()),
        containsInAnyOrder(stewards.toArray()));

    assertThat(
        "Reader added on snapshot creation, duplicates removed without error",
        rolesToPolicies.get(IamRole.READER.toString()),
        contains(readerEmail));

    assertThat(
        "Discoverer added on snapshot creation",
        rolesToPolicies.get(IamRole.DISCOVERER.toString()),
        contains(discovererEmail));

    // Test enabling secure monitoring on existing project
    assertThat("Secure monitoring should be disabled", not(dataset.isSecureMonitoringEnabled()));
    assertThat("Job completes", dataRepoFixtures.enableSecureMonitoring(steward(), datasetId));
    assertThat(
        "Secure monitoring should now be enabled",
        dataRepoFixtures.getDataset(steward(), datasetId).isSecureMonitoringEnabled());

    // Test disabling secure monitoring on existing project
    assertThat("Job completes", dataRepoFixtures.disableSecureMonitoring(steward(), datasetId));
    assertFalse(
        "Secure monitoring should now be disabled",
        dataRepoFixtures.getDataset(steward(), datasetId).isSecureMonitoringEnabled());
  }
}
