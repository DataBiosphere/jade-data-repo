package bio.terra.service.snapshot;

import static bio.terra.common.PdaoConstant.PDAO_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.RelationshipModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotPreviewModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.model.TableModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.tabulardata.google.BigQueryProject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = {"features.search.api=enabled"})
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
@EmbeddedDatabaseTest
public class SnapshotMinimalConnectedTest {

  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private MockMvc mvc;
  @Autowired private ObjectMapper objectMapper;
  @Autowired private JsonLoader jsonLoader;
  @Autowired private DatasetDao datasetDao;
  @Autowired private SnapshotDao snapshotDao;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired private ConfigurationService configService;

  @MockBean private IamProviderInterface samService;

  private BillingProfileModel billingProfile;
  private final Storage storage = StorageOptions.getDefaultInstance().getService();

  @Before
  public void setup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);
    configService.reset();
    billingProfile =
        connectedOperations.createProfileForAccount(testConfig.getGoogleBillingAccountId());
  }

  @After
  public void tearDown() throws Exception {
    connectedOperations.teardown();
    configService.reset();
  }

  @Test
  public void testMinimal() throws Exception {
    DatasetSummaryModel datasetMinimalSummary = setupMinimalDataset();
    String datasetName = PDAO_PREFIX + datasetMinimalSummary.getName();
    UUID datasetMinimalSummaryDefaultProfileId = datasetMinimalSummary.getDefaultProfileId();
    BigQueryProject bigQueryProject =
        TestUtils.bigQueryProjectForDatasetName(datasetDao, datasetMinimalSummary.getName());
    long datasetParticipants =
        SnapshotConnectedTestUtils.queryForCount(datasetName, "participant", bigQueryProject);
    assertThat("dataset participants loaded properly", datasetParticipants, equalTo(2L));
    long datasetSamples =
        SnapshotConnectedTestUtils.queryForCount(datasetName, "sample", bigQueryProject);
    assertThat("dataset samples loaded properly", datasetSamples, equalTo(5L));

    SnapshotRequestModel snapshotRequest =
        SnapshotConnectedTestUtils.makeSnapshotTestRequest(
            jsonLoader, datasetMinimalSummary, "dataset-minimal-snapshot.json", null);
    assertThat(
        "SnapshotRequestModel profileId is empty", snapshotRequest.getProfileId(), equalTo(null));
    MockHttpServletResponse response =
        SnapshotConnectedTestUtils.performCreateSnapshot(
            connectedOperations, mvc, snapshotRequest, "");
    SnapshotSummaryModel summaryModel =
        SnapshotConnectedTestUtils.validateSnapshotCreated(
            connectedOperations, snapshotRequest, response);
    SnapshotModel snapshotModel =
        SnapshotConnectedTestUtils.getTestSnapshot(
            mvc, objectMapper, summaryModel.getId(), snapshotRequest, datasetMinimalSummary);
    List<TableModel> tables = snapshotModel.getTables();
    Optional<TableModel> participantTable =
        tables.stream().filter(t -> t.getName().equals("participant")).findFirst();
    Optional<TableModel> sampleTable =
        tables.stream().filter(t -> t.getName().equals("sample")).findFirst();
    assertThat("participant table exists", participantTable.isPresent(), equalTo(true));
    assertThat("sample table exists", sampleTable.isPresent(), equalTo(true));
    assertThat(
        "defaultProfileId from dataset was used for snapshot",
        snapshotModel.getProfileId(),
        equalTo(datasetMinimalSummaryDefaultProfileId));

    BigQueryProject bigQuerySnapshotProject =
        TestUtils.bigQueryProjectForSnapshotName(snapshotDao, snapshotModel.getName());

    long snapshotParticipants =
        SnapshotConnectedTestUtils.queryForCount(
            summaryModel.getName(), "participant", bigQuerySnapshotProject);
    assertThat("dataset participants loaded properly", snapshotParticipants, equalTo(1L));
    assertThat(
        "participant row count matches expectation",
        participantTable.get().getRowCount(),
        equalTo(1));
    long snapshotSamples =
        SnapshotConnectedTestUtils.queryForCount(
            summaryModel.getName(), "sample", bigQuerySnapshotProject);
    assertThat("dataset samples loaded properly", snapshotSamples, equalTo(2L));
    assertThat("sample row count matches expectation", sampleTable.get().getRowCount(), equalTo(2));
    List<RelationshipModel> relationships = snapshotModel.getRelationships();
    assertThat("a relationship comes back", relationships.size(), equalTo(1));
    RelationshipModel relationshipModel = relationships.get(0);
    assertThat(
        "relationship name is right", relationshipModel.getName(), equalTo("participant_sample"));
    assertThat(
        "from table is right", relationshipModel.getFrom().getTable(), equalTo("participant"));
    assertThat("from column is right", relationshipModel.getFrom().getColumn(), equalTo("id"));
    assertThat("to table is right", relationshipModel.getTo().getTable(), equalTo("sample"));
    assertThat(
        "to column is right", relationshipModel.getTo().getColumn(), equalTo("participant_id"));

    SnapshotPreviewModel snapshotPreviewModel =
        SnapshotConnectedTestUtils.getTablePreview(
            connectedOperations, summaryModel.getId(), "participant", 10, 0, null, null);

    assertThat(
        "participant has the correct age",
        ((LinkedHashMap) snapshotPreviewModel.getResult().get(0)).get("age"),
        equalTo("23"));
  }

  @Test
  public void testDatasetAndSnapshotDataView() throws Exception {
    DatasetSummaryModel datasetMinimalSummary = setupMinimalDataset();
    validateDataView(
        ConnectedOperations.TDRResourceType.DATASET,
        datasetMinimalSummary.getId(),
        datasetMinimalSummary.getName());

    UUID datasetMinimalSummaryProfileId = datasetMinimalSummary.getDefaultProfileId();
    SnapshotRequestModel snapshotRequest =
        SnapshotConnectedTestUtils.makeSnapshotTestRequest(
            jsonLoader,
            datasetMinimalSummary,
            "dataset-minimal-snapshot-full-view.json",
            datasetMinimalSummary.getDefaultProfileId());

    assertThat(
        "dataset default profile Id was included in snapshot request",
        snapshotRequest.getProfileId(),
        equalTo(datasetMinimalSummaryProfileId));

    MockHttpServletResponse response =
        SnapshotConnectedTestUtils.performCreateSnapshot(
            connectedOperations, mvc, snapshotRequest, "");
    SnapshotSummaryModel summaryModel =
        SnapshotConnectedTestUtils.validateSnapshotCreated(
            connectedOperations, snapshotRequest, response);

    validateDataView(
        ConnectedOperations.TDRResourceType.SNAPSHOT, summaryModel.getId(), summaryModel.getName());
  }

  private void validateDataView(
      ConnectedOperations.TDRResourceType resourceType, UUID resourceId, String resourceName)
      throws Exception {
    List<Object> ageGreaterThanOneResult =
        connectedOperations.retrieveDataSuccess(
            resourceType, resourceId, "participant", 10, 0, "WHERE age > 1", null);

    assertThat(
        "participant dataset data has 2 records", ageGreaterThanOneResult.size(), equalTo(2));

    List<Object> ageGreaterThan23Result =
        connectedOperations.retrieveDataSuccess(
            resourceType, resourceId, "participant", 10, 0, "WHERE age > 23", null);

    assertThat(
        "participant dataset view has one record", ageGreaterThan23Result.size(), equalTo(1));

    String prefix =
        resourceType.equals(ConnectedOperations.TDRResourceType.DATASET) ? PDAO_PREFIX : "";
    String joinClause =
        "JOIN " + prefix + resourceName + ".sample ON " + prefix + "sample.participant_id = id";
    ErrorModel previewError =
        connectedOperations.retrieveDataFailure(
            resourceType,
            resourceId,
            "participant",
            10,
            0,
            joinClause,
            null,
            HttpStatus.INTERNAL_SERVER_ERROR);

    assertTrue(
        "JOIN with sample table fails",
        previewError.getMessage().contains("Failure executing query"));
  }

  @Test
  public void testMinimalBadAsset() throws Exception {
    DatasetSummaryModel datasetMinimalSummary = setupMinimalDataset();
    SnapshotRequestModel snapshotRequest =
        SnapshotConnectedTestUtils.makeSnapshotTestRequest(
            jsonLoader,
            datasetMinimalSummary,
            "dataset-minimal-snapshot-bad-asset.json",
            datasetMinimalSummary.getDefaultProfileId());
    MvcResult result = SnapshotConnectedTestUtils.launchCreateSnapshot(mvc, snapshotRequest, "");
    MockHttpServletResponse response = connectedOperations.validateJobModelAndWait(result);
    assertThat(response.getStatus(), equalTo(HttpStatus.NOT_FOUND.value()));
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
