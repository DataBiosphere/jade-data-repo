package bio.terra.service.snapshot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.springframework.test.util.AssertionErrors.assertFalse;

import bio.terra.common.PdaoConstant;
import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration.User;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.IntegrationTestConfiguration;
import bio.terra.integration.Users;
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
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = IntegrationTestConfiguration.class)
@ActiveProfiles({"google", "integrationtest"})
@Tag(Integration.TAG)
@Execution(ExecutionMode.CONCURRENT)
class SnapshotIntegrationTest {
  @Autowired private JsonLoader jsonLoader;
  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private Users users;

  private static final Logger logger = LoggerFactory.getLogger(SnapshotIntegrationTest.class);

  private final ThreadLocal<Users.TestUsers> testUsers = new ThreadLocal<>();
  private final ThreadLocal<UUID> profileId = new ThreadLocal<>();
  private final ThreadLocal<UUID> datasetId = new ThreadLocal<>();
  private final ThreadLocal<UUID> createdSnapshotId = new ThreadLocal<>();
  final String participantTableName = "participant";
  final int participantTableRowCount = 5;

  private User steward() {
    return testUsers.get().steward();
  }

  private User custodian() {
    return testUsers.get().custodian();
  }

  private User reader() {
    return testUsers.get().reader();
  }

  private User admin() {
    return testUsers.get().admin();
  }

  private User discoverer() {
    return testUsers.get().discoverer();
  }

  @BeforeEach
  public void setup() throws Exception {
    testUsers.set(users.testUsers());
    logger.info("testUsers: {}", testUsers.get());
    profileId.set(dataRepoFixtures.createBillingProfile(steward()).getId());
    dataRepoFixtures.addPolicyMember(
        steward(), profileId.get(), IamRole.USER, custodian().email(), IamResourceType.SPEND_PROFILE);

    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId.get(), "ingest-test-dataset.json");
    datasetId.set(datasetSummaryModel.getId());
    dataRepoFixtures.addDatasetPolicyMember(
        steward(), datasetId.get(), IamRole.CUSTODIAN, custodian().email());

    IngestRequestModel request =
        dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-participant.json");
    dataRepoFixtures.ingestJsonData(steward(), datasetId.get(), request);
    request = dataRepoFixtures.buildSimpleIngest("sample", "ingest-test/ingest-test-sample.json");
    dataRepoFixtures.ingestJsonData(steward(), datasetId.get(), request);
  }

  @AfterEach
  public void tearDown() throws Exception {
    var snapshot = createdSnapshotId.get();
    if (snapshot != null) {
      try {
        dataRepoFixtures.deleteSnapshot(steward(), snapshot);
      } catch (Exception | AssertionError ex) {
        logger.warn("cleanup failed when deleting snapshot " + snapshot, ex);
      }
    }

    if (datasetId.get() != null) {
      dataRepoFixtures.deleteDatasetLog(steward(), datasetId.get());
    }

    if (profileId.get() != null) {
      dataRepoFixtures.deleteProfileLog(steward(), profileId.get());
    }
  }

  @Test
  void snapshotRowIdsHappyPathTest() throws Exception {
    // fetch rowIds from the ingested dataset by querying the participant table
    User steward = steward();
    DatasetModel dataset = dataRepoFixtures.getDataset(steward, datasetId.get());
    String participantTable = "participant";
    String sampleTable = "sample";

    List<Object> participantResults =
        dataRepoFixtures
            .retrieveDatasetData(steward, datasetId.get(), participantTable, 0, 1000, null)
            .getResult();
    List<UUID> participantIds =
        participantResults.stream()
            .map(
                r ->
                    UUID.fromString(
                        ((Map<?, ?>) r).get(PdaoConstant.PDAO_ROW_ID_COLUMN).toString()))
            .toList();
    List<Object> sampleResults =
        dataRepoFixtures
            .retrieveDatasetData(steward, datasetId.get(), sampleTable, 0, 1000, null)
            .getResult();
    List<UUID> sampleIds =
        sampleResults.stream()
            .map(
                r ->
                    UUID.fromString(
                        ((Map<?, ?>) r).get(PdaoConstant.PDAO_ROW_ID_COLUMN).toString()))
            .toList();

    // swap in these row ids in the request
    SnapshotRequestModel requestModel =
        jsonLoader.loadObject("ingest-test-snapshot-row-ids-test.json", SnapshotRequestModel.class);
    requestModel.getContents().get(0).getRowIdSpec().getTables().get(0).setRowIds(participantIds);
    requestModel.getContents().get(0).getRowIdSpec().getTables().get(1).setRowIds(sampleIds);

    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshotWithRequest(
            steward, dataset.getName(), profileId.get(), requestModel);
    createdSnapshotId.set(snapshotSummary.getId());
    SnapshotModel snapshot =
        Awaitility.waitAtMost(Duration.ofSeconds(10))
            .until(
                () -> dataRepoFixtures.getSnapshot(steward, snapshotSummary.getId(), null),
                Objects::nonNull);
    assertThat("new snapshot has been created", snapshot.getName(), is(requestModel.getName()));
    assertThat(
        "new snapshot has the correct number of tables",
        snapshot.getTables(),
        hasSize(requestModel.getContents().get(0).getRowIdSpec().getTables().size()));
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
        dataRepoFixtures.retrieveUserSnapshotRoles(steward, snapshotSummary.getId());
    assertThat("The Steward was given steward access", stewardRoles, hasItem("steward"));
  }

  @Test
  void snapshotByQueryHappyPathTest() throws Exception {
    User steward = steward();
    DatasetModel dataset = dataRepoFixtures.getDataset(steward, datasetId.get());
    SnapshotRequestModel requestModel = snapshotByQueryRequestModel(dataset);
    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshotWithRequest(
            steward, dataset.getName(), profileId.get(), requestModel);
    createdSnapshotId.set(snapshotSummary.getId());
    SnapshotModel snapshot =
        Awaitility.waitAtMost(Duration.ofSeconds(10))
            .until(
                () -> dataRepoFixtures.getSnapshot(steward, snapshotSummary.getId(), null),
                Objects::nonNull);
    assertThat("new snapshot has been created", snapshot.getName(), is(requestModel.getName()));
  }

  @Test
  void snapshotByAssetHappyPathTest() throws Exception {
    User steward = steward();
    DatasetModel dataset = dataRepoFixtures.getDataset(steward, datasetId.get());
    String datasetName = dataset.getName();
    SnapshotRequestModel requestModel =
        jsonLoader.loadObject("ingest-test-snapshot-asset.json", SnapshotRequestModel.class);
    // swap in the correct dataset name (with the id at the end)
    requestModel.getContents().get(0).setDatasetName(datasetName);
    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshotWithRequest(
            steward, dataset.getName(), profileId.get(), requestModel);
    createdSnapshotId.set(snapshotSummary.getId());
    SnapshotModel snapshot =
        Awaitility.waitAtMost(Duration.ofSeconds(10))
            .until(
                () -> dataRepoFixtures.getSnapshot(steward, snapshotSummary.getId(), null),
                Objects::nonNull);
    assertThat("new snapshot has been created", snapshot.getName(), is(requestModel.getName()));
  }

  @Test
  void deleteAssetWithSnapshotTest() throws Exception {
    User steward = steward();
    DatasetModel dataset = dataRepoFixtures.getDataset(steward, datasetId.get());
    SnapshotRequestModel requestModel = snapshotByQueryRequestModel(dataset);
    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshotWithRequest(
            steward, dataset.getName(), profileId.get(), requestModel);
    Awaitility.waitAtMost(Duration.ofSeconds(10))
        .until(() -> dataRepoFixtures.getSnapshot(steward, snapshotSummary.getId(), null) != null);
    ErrorModel errorModel =
        dataRepoFixtures.deleteDatasetAssetExpectFailure(steward, dataset.getId(), "sample_centric");
    assertThat(
        "Error deleting asset",
        errorModel.getMessage(),
        containsString("The asset is being used by snapshots: " + snapshotSummary.getId()));
    dataRepoFixtures.deleteSnapshot(steward, snapshotSummary.getId());
    dataRepoFixtures.deleteDatasetAsset(steward, dataset.getId(), "sample_centric");
  }

  @Test
  void retrieveRowCountAndSnapshotByFullViewTest() throws Exception {
    // DATASET
    User steward = steward();
    DatasetModel dataset = dataRepoFixtures.getDataset(steward, datasetId.get());
    String datasetName = dataset.getName();

    // Empty dataset table
    dataRepoFixtures.assertDatasetTableCount(steward, dataset, "file", 0);

    // Non-empty dataset table, no filtering: total row count = filtered row count > 0
    dataRepoFixtures.assertDatasetTableCount(
        steward, dataset, participantTableName, participantTableRowCount);

    // Non-empty dataset table, filtered results: total row count > filtered row count > 0
    DatasetDataModel filteredDatasetDataModel =
        dataRepoFixtures.retrieveDatasetData(
            steward,
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
            steward,
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
        dataRepoFixtures.createSnapshotWithRequest(
            steward, datasetName, profileId.get(), requestModel);
    createdSnapshotId.set(snapshotSummary.getId());
    SnapshotModel snapshot =
        Awaitility.waitAtMost(Duration.ofSeconds(10))
            .until(
                () -> dataRepoFixtures.getSnapshot(steward, snapshotSummary.getId(), null),
                Objects::nonNull);
    assertThat("new snapshot has been created", snapshot.getName(), is(requestModel.getName()));
    assertThat("the relationship comes through", snapshot.getRelationships(), hasSize(1));

    // Empty snapshot table
    dataRepoFixtures.assertSnapshotTableCount(steward, snapshot, "file", 0);

    // Non-empty snapshot table, no filtering: total row count = filtered row count > 0
    dataRepoFixtures.assertSnapshotTableCount(
        steward, snapshot, participantTableName, participantTableRowCount);

    // Non-empty snapshot table, filtered results: total row count > filtered row count > 0
    SnapshotPreviewModel filteredSnapshotPreviewModel =
        dataRepoFixtures.retrieveSnapshotPreviewById(
            steward,
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
            steward,
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
  void snapshotByFullViewAndPetServiceAccountHappyPathTest() throws Exception {
    User steward = steward();
    DatasetModel dataset = dataRepoFixtures.getDataset(steward, datasetId.get());
    String datasetName = dataset.getName();
    SnapshotRequestModel requestModel =
        jsonLoader.loadObject("ingest-test-snapshot-fullviews.json", SnapshotRequestModel.class);
    // swap in the correct dataset name (with the id at the end)
    requestModel.getContents().get(0).setDatasetName(datasetName);
    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshotWithRequest(
            steward, datasetName, profileId.get(), requestModel, true, true);
    createdSnapshotId.set(snapshotSummary.getId());
    SnapshotModel snapshot =
        Awaitility.waitAtMost(Duration.ofSeconds(10))
            .until(
                () -> dataRepoFixtures.getSnapshot(steward, snapshotSummary.getId(), null),
                Objects::nonNull);
    assertThat("new snapshot has been created", snapshot.getName(), is(requestModel.getName()));
    assertThat("the relationship comes through", snapshot.getRelationships(), hasSize(1));
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
  void testCreateSnapshotWithPolicies() throws Exception {
    User steward = steward();
    DatasetModel dataset = dataRepoFixtures.getDataset(steward, datasetId.get());
    String datasetName = dataset.getName();
    SnapshotRequestModel requestModel =
        jsonLoader.loadObject("ingest-test-snapshot-fullviews.json", SnapshotRequestModel.class);
    // swap in the correct dataset name (with the id at the end)
    requestModel.getContents().get(0).setDatasetName(datasetName);

    List<String> stewards = List.of(steward.email(), admin().email());
    String readerEmail = reader().email();
    List<String> readersWithDuplicates = List.of(readerEmail, readerEmail);
    String discovererEmail = discoverer().email();
    SnapshotRequestModelPolicies policiesRequest =
        new SnapshotRequestModelPolicies()
            .stewards(stewards)
            .readers(readersWithDuplicates)
            .addDiscoverersItem(discovererEmail);
    requestModel.setPolicies(policiesRequest);

    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshotWithRequest(
            steward, datasetName, profileId.get(), requestModel);
    UUID snapshotId = snapshotSummary.getId();
    createdSnapshotId.set(snapshotId);
    Awaitility.waitAtMost(Duration.ofSeconds(10))
        .until(() -> dataRepoFixtures.getSnapshot(steward, snapshotSummary.getId(), null) != null);

    Map<String, List<String>> rolesToPolicies =
        dataRepoFixtures.retrieveSnapshotPolicies(steward, snapshotId).getPolicies().stream()
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
    assertThat("Job completes", dataRepoFixtures.enableSecureMonitoring(steward, dataset.getId()));
    assertThat(
        "Secure monitoring should now be enabled",
        dataRepoFixtures.getDataset(steward, dataset.getId()).isSecureMonitoringEnabled());

    // Test disabling secure monitoring on existing project
    assertThat("Job completes", dataRepoFixtures.disableSecureMonitoring(steward, dataset.getId()));
    assertFalse(
        "Secure monitoring should now be disabled",
        dataRepoFixtures.getDataset(steward, dataset.getId()).isSecureMonitoringEnabled());
  }
}
