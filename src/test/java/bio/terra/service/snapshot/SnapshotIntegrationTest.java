package bio.terra.service.snapshot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

import bio.terra.common.PdaoConstant;
import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.integration.BigQueryFixtures;
import bio.terra.integration.DataRepoClient;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.TestJobWatcher;
import bio.terra.integration.UsersBase;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableResult;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
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

  @Rule @Autowired public TestJobWatcher testWatcher;

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
    String datasetProject = dataset.getDataProject();
    String bqDatasetName = PdaoConstant.PDAO_PREFIX + dataset.getName();
    String participantTable = "participant";
    String sampleTable = "sample";
    BigQuery bigQuery = BigQueryFixtures.getBigQuery(dataset.getDataProject(), stewardToken);
    String sql =
        String.format(
            "SELECT %s FROM `%s.%s.%s`",
            PdaoConstant.PDAO_ROW_ID_COLUMN, datasetProject, bqDatasetName, participantTable);
    TableResult participantIds = BigQueryFixtures.query(sql, bigQuery);
    List<UUID> participantIdList =
        StreamSupport.stream(participantIds.getValues().spliterator(), false)
            .map(v -> UUID.fromString(v.get(0).getStringValue()))
            .collect(Collectors.toList());
    sql =
        String.format(
            "SELECT %s FROM `%s.%s.%s`",
            PdaoConstant.PDAO_ROW_ID_COLUMN, datasetProject, bqDatasetName, sampleTable);
    TableResult sampleIds = BigQueryFixtures.query(sql, bigQuery);
    List<UUID> sampleIdList =
        StreamSupport.stream(sampleIds.getValues().spliterator(), false)
            .map(v -> UUID.fromString(v.get(0).getStringValue()))
            .collect(Collectors.toList());

    // swap in these row ids in the request
    SnapshotRequestModel requestModel =
        jsonLoader.loadObject("ingest-test-snapshot-row-ids-test.json", SnapshotRequestModel.class);
    requestModel
        .getContents()
        .get(0)
        .getRowIdSpec()
        .getTables()
        .get(0)
        .setRowIds(participantIdList);
    requestModel.getContents().get(0).getRowIdSpec().getTables().get(1).setRowIds(sampleIdList);

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
  public void snapshotByFullViewHappyPathTest() throws Exception {
    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
    String datasetName = dataset.getName();
    SnapshotRequestModel requestModel =
        jsonLoader.loadObject("ingest-test-snapshot-fullviews.json", SnapshotRequestModel.class);
    // swap in the correct dataset name (with the id at the end)
    requestModel.getContents().get(0).setDatasetName(datasetName);
    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshotWithRequest(steward(), datasetName, profileId, requestModel);
    TimeUnit.SECONDS.sleep(10);
    createdSnapshotIds.add(snapshotSummary.getId());
    SnapshotModel snapshot = dataRepoFixtures.getSnapshot(steward(), snapshotSummary.getId(), null);
    assertEquals("new snapshot has been created", snapshot.getName(), requestModel.getName());
    assertEquals("the relationship comes through", 1, snapshot.getRelationships().size());
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
}
