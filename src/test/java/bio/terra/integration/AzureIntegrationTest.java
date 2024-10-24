package bio.terra.integration;

import static bio.terra.service.filedata.azure.util.AzureBlobIOTestUtility.MIB;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.app.model.AzureCloudResource;
import bio.terra.app.model.AzureRegion;
import bio.terra.common.CollectionType;
import bio.terra.common.TestUtils;
import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.common.configuration.TestConfiguration.User;
import bio.terra.common.fixtures.DatasetFixtures;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.model.AccessInfoParquetModel;
import bio.terra.model.AccessInfoParquetModelTable;
import bio.terra.model.BulkLoadArrayRequestModel;
import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadFileResultModel;
import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.model.BulkLoadHistoryModelList;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.model.BulkLoadResultModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.ColumnModel;
import bio.terra.model.ColumnStatisticsIntModel;
import bio.terra.model.DRSAccessMethod;
import bio.terra.model.DRSAccessURL;
import bio.terra.model.DRSObject;
import bio.terra.model.DatasetDataModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestAccessIncludeModel;
import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.model.DatasetSchemaUpdateModelChanges;
import bio.terra.model.DatasetSpecificationModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.EnumerateDatasetModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.JobModel;
import bio.terra.model.PolicyModel;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotAccessRequestStatus;
import bio.terra.model.SnapshotBuilderCohort;
import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.model.SnapshotBuilderCountRequest;
import bio.terra.model.SnapshotBuilderCriteria;
import bio.terra.model.SnapshotBuilderCriteriaGroup;
import bio.terra.model.SnapshotBuilderDomainCriteria;
import bio.terra.model.SnapshotBuilderParentConcept;
import bio.terra.model.SnapshotBuilderProgramDataListCriteria;
import bio.terra.model.SnapshotBuilderProgramDataRangeCriteria;
import bio.terra.model.SnapshotExportResponseModel;
import bio.terra.model.SnapshotExportResponseModelFormatParquet;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotPreviewModel;
import bio.terra.model.SnapshotRequestAssetModel;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestModelPolicies;
import bio.terra.model.SnapshotRequestQueryModel;
import bio.terra.model.SnapshotRequestRowIdModel;
import bio.terra.model.SnapshotRequestRowIdTableModel;
import bio.terra.model.SnapshotRetrieveIncludeModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.model.SqlSortDirectionAscDefault;
import bio.terra.model.StorageResourceModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamNotFoundException;
import bio.terra.service.filedata.DrsId;
import bio.terra.service.filedata.DrsIdService;
import bio.terra.service.filedata.DrsResponse;
import bio.terra.service.filedata.azure.util.AzureBlobIOTestUtility;
import bio.terra.service.filedata.google.util.GcsBlobIOTestUtility;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.loganalytics.LogAnalyticsManager;
import com.azure.resourcemanager.loganalytics.models.Workspace;
import com.azure.resourcemanager.storage.models.StorageAccount;
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.ResourceUtils;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@AutoConfigureMockMvc
@Category(Integration.class)
@Ignore("DCJ-826: Temporarily disabled due to missing Azure resources")
public class AzureIntegrationTest extends UsersBase {
  private static final Logger logger = LoggerFactory.getLogger(AzureIntegrationTest.class);

  private static final String omopDatasetName = "it_dataset_omop";
  private static final String omopDatasetDesc =
      "OMOP schema based on BigQuery schema from https://github.com/OHDSI/CommonDataModel/wiki with extra columns suffixed with _custom";

  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private SamFixtures samFixtures;
  @Autowired private AuthService authService;
  @Autowired private TestConfiguration testConfig;
  @Autowired private AzureResourceConfiguration azureResourceConfiguration;
  @Autowired private JsonLoader jsonLoader;

  private User steward;
  private User admin;
  private User researcher;
  private UUID datasetId;
  private UUID releaseSnapshotId;
  private String datasetName;
  private List<UUID> snapshotIds;
  private String dac;

  private List<UUID> snapshotAccessRequestIds;
  private UUID profileId;
  private AzureBlobIOTestUtility azureBlobIOTestUtility;
  private GcsBlobIOTestUtility gcsBlobIOTestUtility;
  private Set<String> storageAccounts;

  @Before
  public void setup() throws Exception {
    super.setup(false);
    // Voldemort is required by this test since the application is deployed with his user authz'ed
    steward = steward("voldemort");
    admin = admin("hermione");
    researcher = reader("harry");
    dataRepoFixtures.resetConfig(steward);
    profileId = dataRepoFixtures.createAzureBillingProfile(steward).getId();
    RequestRetryOptions retryOptions =
        new RequestRetryOptions(
            RetryPolicyType.EXPONENTIAL,
            azureResourceConfiguration.maxRetries(),
            azureResourceConfiguration.retryTimeoutSeconds(),
            null,
            null,
            null);
    azureBlobIOTestUtility =
        new AzureBlobIOTestUtility(
            azureResourceConfiguration.getAppToken(testConfig.getTargetTenantId()),
            testConfig.getSourceStorageAccountName(),
            null,
            retryOptions);
    gcsBlobIOTestUtility = new GcsBlobIOTestUtility(testConfig.getIngestbucket(), null);
    snapshotIds = new ArrayList<>();
    snapshotAccessRequestIds = new ArrayList<>();
    storageAccounts = new TreeSet<>();
  }

  @After
  public void teardown() throws Exception {
    if (releaseSnapshotId != null) {
      snapshotIds.add(releaseSnapshotId);
    }
    logger.info(
        "Teardown: trying to delete snapshots {}, dataset {}, billing profile {}",
        snapshotIds,
        datasetId,
        profileId);

    dataRepoFixtures.resetConfig(steward);

    for (UUID snapshotAccessRequestId : snapshotAccessRequestIds) {
      samFixtures.deleteSnapshotAccessRequest(steward, snapshotAccessRequestId);
    }

    for (UUID snapshotId : snapshotIds) {
      dataRepoFixtures.deleteSnapshot(steward, snapshotId);
    }
    if (datasetId != null) {
      dataRepoFixtures.deleteDataset(steward, datasetId);
    }
    if (profileId != null) {
      // TODO - https://broadworkbench.atlassian.net/browse/DCJ-228
      // As we move towards running smoke and integration tests in BEEs, we would like to do so
      // without relying (directly or indirectly) on a pre-seeded Admin Firecloud group set via
      // property sam.adminsGroupEmail.
      // Only Datarepo admins are allowed to delete profiles with deleteCloudResources=true,
      // so this test currently relies on the presence of this group to successfully clean up its
      // billing profile.
      // If we are instead leveraging Janitor via Cloud Resource Library to clean up Azure resources
      // generated by our integration tests, its owner can delete the profile in the standard way,
      // rather than needing an admin to do it.
      dataRepoFixtures.deleteProfileWithCloudResourceDelete(admin, profileId);
    }
    if (storageAccounts != null) {
      storageAccounts.forEach(this::deleteCloudResources);
    }
    azureBlobIOTestUtility.teardown();
    gcsBlobIOTestUtility.teardown();

    if (dac != null) {
      samFixtures.deleteGroup(steward, dac);
    }
  }

  @Test
  public void datasetsHappyPath() throws Exception {
    // Note: this region should not be the same as the default region in the application deployment
    // (eastus by default)
    AzureRegion region = AzureRegion.SOUTH_CENTRAL_US;

    DatasetSummaryModel summaryModel =
        dataRepoFixtures.createDataset(
            steward, profileId, "omop/it-dataset-omop.json", CloudPlatform.AZURE, false, region);
    datasetId = summaryModel.getId();
    String storageAccountName = recordStorageAccount(steward, CollectionType.DATASET, datasetId);
    logger.info("dataset id is " + summaryModel.getId());
    assertThat(summaryModel.getName(), startsWith(omopDatasetName));
    assertThat(summaryModel.getDescription(), equalTo(omopDatasetDesc));

    DatasetModel datasetModel = dataRepoFixtures.getDataset(steward, summaryModel.getId());

    assertThat(datasetModel.getName(), startsWith(omopDatasetName));
    assertThat(datasetModel.getDescription(), equalTo(omopDatasetDesc));

    // There is a delay from when a resource is created in SAM to when it is available in an
    // enumerate call.
    boolean metExpectation =
        TestUtils.eventualExpect(
            5,
            60,
            true,
            () -> {
              EnumerateDatasetModel enumerateDatasetModel =
                  dataRepoFixtures.enumerateDatasets(steward);
              boolean found = false;
              for (DatasetSummaryModel oneDataset : enumerateDatasetModel.getItems()) {
                if (oneDataset.getId().equals(datasetModel.getId())) {
                  assertThat(oneDataset.getName(), startsWith(omopDatasetName));
                  assertThat(oneDataset.getDescription(), equalTo(omopDatasetDesc));
                  Map<String, StorageResourceModel> storageMap =
                      datasetModel.getStorage().stream()
                          .collect(
                              Collectors.toMap(
                                  StorageResourceModel::getCloudResource, Function.identity()));

                  // Verify that the real region of the storage account and the log analytics
                  // workspace matches the region of the dataset
                  verifyCloudResourceRegions(storageAccountName, region);

                  // TODO<DR-3163> The values are currently not necessarily correct
                  assertThat(
                      "Application deployment matches",
                      storageMap.entrySet().stream()
                          .filter(
                              e ->
                                  e.getKey()
                                      .equals(AzureCloudResource.APPLICATION_DEPLOYMENT.getValue()))
                          .findAny()
                          .map(e -> e.getValue().getRegion())
                          .orElseThrow(() -> new RuntimeException("Key not found")),
                      equalTo(region.getValue()));

                  assertThat(
                      "Synapse matches",
                      storageMap.entrySet().stream()
                          .filter(
                              e ->
                                  e.getKey()
                                      .equals(AzureCloudResource.SYNAPSE_WORKSPACE.getValue()))
                          .findAny()
                          .map(e -> e.getValue().getRegion())
                          .orElseThrow(() -> new RuntimeException("Key not found")),
                      equalTo(region.getValue()));

                  assertThat(
                      "Storage account storage matches",
                      storageMap.entrySet().stream()
                          .filter(
                              e -> e.getKey().equals(AzureCloudResource.STORAGE_ACCOUNT.getValue()))
                          .findAny()
                          .map(e -> e.getValue().getRegion())
                          .orElseThrow(() -> new RuntimeException("Key not found")),
                      equalTo(region.getValue()));

                  assertThat(
                      "dataset summary has Azure cloud platform",
                      oneDataset.getCloudPlatform(),
                      equalTo(CloudPlatform.AZURE));

                  assertThat(
                      "dataset summary has storage account",
                      oneDataset.getStorageAccount(),
                      notNullValue());

                  assertThat(
                      "No Google storage resources are included",
                      storageMap.values().stream()
                          .map(StorageResourceModel::getCloudResource)
                          .collect(Collectors.toSet()),
                      equalTo(
                          Set.of(
                              AzureCloudResource.STORAGE_ACCOUNT.getValue(),
                              AzureCloudResource.SYNAPSE_WORKSPACE.getValue(),
                              AzureCloudResource.APPLICATION_DEPLOYMENT.getValue())));

                  found = true;
                  break;
                }
              }
              return found;
            });

    assertTrue("dataset was found in enumeration", metExpectation);

    // This should fail since it currently has dataset storage account within
    assertThrows(AssertionError.class, () -> dataRepoFixtures.deleteProfile(steward, profileId));

    // Create and delete a dataset and make sure that the profile still can't be deleted
    DatasetSummaryModel summaryModel2 =
        dataRepoFixtures.createDataset(
            steward, profileId, "omop/it-dataset-omop.json", CloudPlatform.AZURE);
    recordStorageAccount(steward, CollectionType.DATASET, datasetId);
    dataRepoFixtures.deleteDataset(steward, summaryModel2.getId());
    assertThat(
        "Original dataset is still there",
        dataRepoFixtures
            .getDatasetRaw(steward, summaryModel.getId())
            .getStatusCode()
            .is2xxSuccessful(),
        equalTo(true));
    assertThat(
        "New dataset was deleted",
        dataRepoFixtures.getDatasetRaw(steward, summaryModel2.getId()).getStatusCode().value(),
        equalTo(404));
    assertThrows(AssertionError.class, () -> dataRepoFixtures.deleteProfile(steward, profileId));
  }

  record IngestSource(String tableName, String ingestFile, long expectedRowCount) {}

  static final List<IngestSource> TABLES =
      List.of(
          new IngestSource("concept", "omop/concept-table-data.jsonl", 7),
          new IngestSource("person", "omop/person-table-data.jsonl", 23),
          new IngestSource("relationship", "omop/relationship.jsonl", 2),
          new IngestSource("concept_ancestor", "omop/concept-ancestor-table-data.jsonl", 10),
          new IngestSource(
              "condition_occurrence", "omop/condition-occurrence-table-data.jsonl", 53),
          new IngestSource(
              "concept_relationship", "omop/concept-relationship-table-data.jsonl", 4));

  class Ingester {
    private final IngestSource source;
    private DataRepoResponse<JobModel> result;

    Ingester(IngestSource source) {
      this.source = source;
    }

    void ingest() throws Exception {
      String tableName = source.tableName();
      List<Map<String, Object>> data =
          jsonLoader.loadObjectAsStream(source.ingestFile(), new TypeReference<>() {});
      var ingestRequestArray =
          dataRepoFixtures
              .buildSimpleIngest(tableName, data)
              .profileId(profileId)
              .ignoreUnknownValues(true);
      result = dataRepoFixtures.ingestJsonDataLaunch(steward, datasetId, ingestRequestArray);
    }

    void waitForCompletion() throws Exception {
      var ingestResult =
          dataRepoFixtures.waitForIngestResponse(steward, result).getResponseObject().orElseThrow();
      assertThat("row count matches", ingestResult.getRowCount(), equalTo(source.expectedRowCount));
    }
  }

  private void populateOmopTable() throws Exception {
    DatasetSummaryModel summaryModel =
        dataRepoFixtures.createDataset(
            steward, profileId, "omop/it-dataset-omop.json", CloudPlatform.AZURE);
    datasetId = summaryModel.getId();
    datasetName = summaryModel.getName();
    recordStorageAccount(steward, CollectionType.DATASET, datasetId);

    // Ingest Tabular data
    var ingesters = TABLES.stream().map(Ingester::new).toList();
    for (Ingester ingester : ingesters) {
      ingester.ingest();
    }
    for (Ingester ingester : ingesters) {
      ingester.waitForCompletion();
    }

    // Create a snapshot
    SnapshotRequestModel requestSnapshotRelease =
        jsonLoader.loadObject("omop/release-snapshot-request.json", SnapshotRequestModel.class);
    requestSnapshotRelease.getContents().get(0).datasetName(summaryModel.getName());
    requestSnapshotRelease.setPolicies(
        new SnapshotRequestModelPolicies().addAggregateDataReadersItem(researcher.getEmail()));

    SnapshotSummaryModel snapshotSummaryAll =
        dataRepoFixtures.createSnapshotWithRequest(
            steward, summaryModel.getName(), profileId, requestSnapshotRelease);
    UUID snapshotByFullViewId = snapshotSummaryAll.getId();
    releaseSnapshotId = snapshotByFullViewId;
    recordStorageAccount(steward, CollectionType.SNAPSHOT, snapshotByFullViewId);
    assertThat(
        "Snapshot exists", snapshotSummaryAll.getName(), equalTo(requestSnapshotRelease.getName()));

    // Add settings to snapshot
    dataRepoFixtures.updateSettings(steward, releaseSnapshotId, "omop/settings.json");
  }

  @Test
  public void testSnapshotCreateFromRequest() throws Exception {
    populateOmopTable();

    SnapshotAccessRequestResponse approvedSnapshotAccessRequest =
        approveSnapshotAccessRequest(makeSnapshotAccessRequest().getId());

    SnapshotSummaryModel snapshotSummaryByRequest =
        makeSnapshotFromRequest(approvedSnapshotAccessRequest);

    String columnName = "datarepo_row_id";
    List<Object> personSnapshotRows =
        dataRepoFixtures
            .retrieveSnapshotPreviewById(
                steward, snapshotSummaryByRequest.getId(), "person", 0, 100, null, columnName)
            .getResult();
    List<Object> conditionOccurrenceSnapshotRows =
        dataRepoFixtures
            .retrieveSnapshotPreviewById(
                steward,
                snapshotSummaryByRequest.getId(),
                "condition_occurrence",
                0,
                100,
                null,
                columnName)
            .getResult();
    List<Object> conceptSnapshotRows =
        dataRepoFixtures
            .retrieveSnapshotPreviewById(
                steward, snapshotSummaryByRequest.getId(), "concept", 0, 100, null, columnName)
            .getResult();
    assertThat(personSnapshotRows, hasSize(23));
    // full table has 53 rows but only 49 map to existing person ids
    assertThat(conditionOccurrenceSnapshotRows, hasSize(49));
    // full table has 7 rows but only 5 are in the condition_occurrence table
    assertThat(conceptSnapshotRows, hasSize(5));

    // assert the snapshot access request has been updated
    SnapshotAccessRequestResponse updatedSnapshotAccessRequest =
        dataRepoFixtures.getSnapshotAccessRequest(steward, approvedSnapshotAccessRequest.getId());
    assertNotNull(
        "Snapshot access request flightId is set", updatedSnapshotAccessRequest.getFlightid());
    assertThat(
        "Snapshot access request createdSnapshotId is correct",
        updatedSnapshotAccessRequest.getCreatedSnapshotId(),
        is(snapshotSummaryByRequest.getId()));

    // ==== Confirm Sam group creation and policies were added as expected for snapshot byRequestId
    // ===
    var expectedSamGroup =
        IamService.constructSamGroupName(snapshotSummaryByRequest.getId().toString());
    // (1) Confirm that a Sam group was created for this snapshot and that both the steward and the
    // researcher are on the group by successfully retrieving the group
    var groupEmail = samFixtures.getGroup(steward, expectedSamGroup);
    assertThat(
        "Group was successfully created and the steward can access the group",
        groupEmail,
        containsString(expectedSamGroup));
    assertThat(
        "Group was successfully created and the researcher can access the group",
        samFixtures.getGroup(researcher, expectedSamGroup),
        containsString(expectedSamGroup));

    // (2) Confirm that the Sam group was added a reader on the snapshot
    var policies =
        dataRepoFixtures.retrieveSnapshotPolicies(steward, snapshotSummaryByRequest.getId());
    Map<String, List<String>> rolesToPolicies =
        policies.getPolicies().stream()
            .collect(Collectors.toMap(PolicyModel::getName, PolicyModel::getMembers));
    assertThat(
        "Group email should have been added as a reader",
        rolesToPolicies.get(IamRole.READER.toString()),
        containsInAnyOrder(groupEmail));

    // (3) Confirm the Sam group was added as the DAC on the snapshot
    assertThat(
        "Group created for snapshot is added as a DAC on the snapshot",
        policies.getAuthDomain(),
        containsInAnyOrder(expectedSamGroup));

    dataRepoFixtures.deleteSnapshot(steward, snapshotSummaryByRequest.getId());
    snapshotIds.remove(snapshotSummaryByRequest.getId());
    // (5) Sam group was also deleted as part of the snapshot delete
    assertThrows(IamNotFoundException.class, () -> samFixtures.getGroup(steward, expectedSamGroup));
  }

  private SnapshotAccessRequestResponse makeSnapshotAccessRequest() throws Exception {
    String filename = "omop/snapshot-access-request.json";
    SnapshotAccessRequestResponse accessRequest =
        dataRepoFixtures.createSnapshotAccessRequest(researcher, releaseSnapshotId, filename);
    assertThat("Snapshot access request exists", accessRequest, notNullValue());
    snapshotAccessRequestIds.add(accessRequest.getId());
    return accessRequest;
  }

  private SnapshotSummaryModel makeSnapshotFromRequest(
      SnapshotAccessRequestResponse snapshotAccessRequestResponse) throws Exception {
    SnapshotRequestModel requestSnapshot =
        jsonLoader.loadObject(
            "omop/snapshot-request-model-by-request-id.json", SnapshotRequestModel.class);
    requestSnapshot.getContents().get(0).setDatasetName(datasetName);
    requestSnapshot
        .getContents()
        .get(0)
        .getRequestIdSpec()
        .setSnapshotRequestId(snapshotAccessRequestResponse.getId());

    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshotWithRequest(
            steward, datasetName, profileId, requestSnapshot);
    UUID snapshotByRequestId = snapshotSummary.getId();
    snapshotIds.add(snapshotByRequestId);
    recordStorageAccount(steward, CollectionType.SNAPSHOT, snapshotByRequestId);
    assertThat(
        "Snapshot exists",
        snapshotSummary.getName(),
        equalTo(
            String.format(
                    "%s_%s",
                    snapshotAccessRequestResponse.getSnapshotName(),
                    snapshotAccessRequestResponse.getId())
                .replace('-', '_')));
    return snapshotSummary;
  }

  private SnapshotAccessRequestResponse approveSnapshotAccessRequest(UUID snapshotRequestId)
      throws Exception {
    SnapshotAccessRequestResponse approvedAccessRequest =
        dataRepoFixtures.approveSnapshotAccessRequest(steward, snapshotRequestId);
    assertThat(
        "Snapshot access request is approved",
        approvedAccessRequest.getStatus(),
        equalTo(SnapshotAccessRequestStatus.APPROVED));
    return approvedAccessRequest;
  }

  @Test
  public void testSnapshotBuilder() throws Exception {
    populateOmopTable();

    var concept1 =
        new SnapshotBuilderConcept().name("concept1").id(1).count(22).code("11").hasChildren(false);
    var concept3 =
        new SnapshotBuilderConcept().name("concept3").id(3).count(24).code("13").hasChildren(true);

    getConceptChildrenTest(concept1, concept3);
    enumerateConceptTest(concept1);
    getConceptHierarchyTest(concept1, concept3);
  }

  private void enumerateConceptTest(SnapshotBuilderConcept concept1) throws Exception {
    var enumerateConceptsResult =
        dataRepoFixtures.enumerateConcepts(steward, releaseSnapshotId, 19, concept1.getName());
    // A concept returned by enumerate concepts always has hasChildren = true, even if it doesn't
    // have children.
    var concept =
        new SnapshotBuilderConcept()
            .name(concept1.getName())
            .id(concept1.getId())
            .count(concept1.getCount())
            .code(concept1.getCode())
            .hasChildren(true);
    assertThat(enumerateConceptsResult.getResult(), CoreMatchers.is(List.of(concept)));
  }

  private void getConceptHierarchyTest(
      SnapshotBuilderConcept concept1, SnapshotBuilderConcept concept3) throws Exception {
    var enumerateConceptsResult =
        dataRepoFixtures.getConceptHierarchy(steward, releaseSnapshotId, 3);
    assertThat(
        enumerateConceptsResult.getResult(),
        CoreMatchers.is(
            List.of(
                new SnapshotBuilderParentConcept()
                    .parentId(2)
                    .children(List.of(concept1, concept3)))));
  }

  private void getConceptChildrenTest(
      SnapshotBuilderConcept concept1, SnapshotBuilderConcept concept3) throws Exception {
    var getConceptResponse = dataRepoFixtures.getConceptChildren(steward, releaseSnapshotId, 2);
    assertThat(getConceptResponse.getResult(), CoreMatchers.is(List.of(concept1, concept3)));

    getCountResponseTest();
    getCountResponseFuzzyValuesTest();
    getCountResponseZeroCaseTest();
  }

  private void getCountResponseTest() throws Exception {
    List<SnapshotBuilderCriteria> criteria =
        List.of(
            new SnapshotBuilderProgramDataListCriteria()
                .values(List.of(0))
                .kind(SnapshotBuilderCriteria.KindEnum.LIST)
                .id(1),
            new SnapshotBuilderProgramDataRangeCriteria()
                .high(1960)
                .low(1940)
                .kind(SnapshotBuilderCriteria.KindEnum.RANGE)
                .id(0),
            new SnapshotBuilderDomainCriteria()
                .conceptId(1)
                .kind(SnapshotBuilderCriteria.KindEnum.DOMAIN)
                .id(19));
    testRollupCountsWithCriteriaAndExpected(criteria, 20);
  }

  private void getCountResponseZeroCaseTest() throws Exception {
    List<SnapshotBuilderCriteria> criteria =
        List.of(
            new SnapshotBuilderProgramDataRangeCriteria()
                .high(1911)
                .low(1911)
                .kind(SnapshotBuilderCriteria.KindEnum.RANGE)
                .id(0));
    testRollupCountsWithCriteriaAndExpected(criteria, 0);
  }

  private void getCountResponseFuzzyValuesTest() throws Exception {
    List<SnapshotBuilderCriteria> criteria =
        List.of(
            new SnapshotBuilderProgramDataListCriteria()
                .values(List.of(8532))
                .kind(SnapshotBuilderCriteria.KindEnum.LIST)
                .id(1));
    testRollupCountsWithCriteriaAndExpected(criteria, 19);
  }

  private void testRollupCountsWithCriteriaAndExpected(
      List<SnapshotBuilderCriteria> criteria, int expectedParticipants) throws Exception {
    SnapshotBuilderCountRequest request =
        new SnapshotBuilderCountRequest()
            .cohorts(
                List.of(
                    new SnapshotBuilderCohort()
                        .criteriaGroups(
                            List.of(
                                new SnapshotBuilderCriteriaGroup()
                                    .meetAll(true)
                                    .mustMeet(true)
                                    .criteria(criteria)))));
    var rollupCountsResponse =
        dataRepoFixtures.getRollupCounts(steward, releaseSnapshotId, request);
    assertThat(rollupCountsResponse.getResult().getTotal(), is(expectedParticipants));
  }

  @Test
  public void datasetIngestFileHappyPath() throws Exception {
    String blobName = "myBlob";
    long fileSize = MIB / 10;
    String sourceFileAzure = azureBlobIOTestUtility.uploadSourceFile(blobName, fileSize);
    String sourceFileGcs = gcsBlobIOTestUtility.uploadSourceFile(blobName, fileSize);
    DatasetSummaryModel summaryModel =
        dataRepoFixtures.createDataset(
            steward, profileId, "omop/it-dataset-omop.json", CloudPlatform.AZURE);
    datasetId = summaryModel.getId();
    recordStorageAccount(steward, CollectionType.DATASET, datasetId);

    Map<String, Integer> tableRowCount = new HashMap<>();

    BulkLoadFileModel fileLoadModel =
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(azureBlobIOTestUtility.createSourcePath(sourceFileAzure))
            .targetPath("/test/target.txt");
    BulkLoadFileModel fileLoadModelAlt1 =
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(azureBlobIOTestUtility.createSourcePath(sourceFileAzure))
            .targetPath("/test/target_alt1.txt");
    BulkLoadFileModel fileLoadModelSas =
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(
                azureBlobIOTestUtility.createSourceSignedPath(
                    sourceFileAzure, getSourceStorageAccountPrimarySharedKey()))
            .targetPath("/test/targetSas.txt");
    BulkLoadFileModel fileLoadModelAlt2 =
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(azureBlobIOTestUtility.createSourcePath(sourceFileAzure))
            .targetPath("/test/target_alt2.txt");
    BulkLoadFileModel fileLoadModelGcs =
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(gcsBlobIOTestUtility.getFullyQualifiedBlobName(sourceFileGcs))
            .targetPath("/test/target_gcs.txt");

    BulkLoadArrayResultModel result =
        dataRepoFixtures.bulkLoadArray(
            steward,
            datasetId,
            new BulkLoadArrayRequestModel()
                .profileId(summaryModel.getDefaultProfileId())
                .loadTag("loadTag")
                .addLoadArrayItem(fileLoadModel)
                .addLoadArrayItem(fileLoadModelAlt1)
                .addLoadArrayItem(fileLoadModelSas)
                .addLoadArrayItem(fileLoadModelAlt2)
                .addLoadArrayItem(fileLoadModelGcs));

    assertThat(result.getLoadSummary().getSucceededFiles(), equalTo(5));

    assertThat(
        "file size matches",
        dataRepoFixtures.getFileByName(steward, datasetId, "/test/target.txt").getSize(),
        equalTo(fileSize));

    assertThat(
        "file with Sas size matches",
        dataRepoFixtures.getFileByName(steward, datasetId, "/test/targetSas.txt").getSize(),
        equalTo(fileSize));

    // lookup file
    List<BulkLoadFileResultModel> loadedFiles = result.getLoadFileResults();
    BulkLoadFileResultModel file1 = loadedFiles.get(0);
    FileModel file1Model = dataRepoFixtures.getFileById(steward, datasetId, file1.getFileId());
    assertThat("Test retrieve file by ID", file1Model.getFileId(), equalTo(file1.getFileId()));

    FileModel file2Model =
        dataRepoFixtures.getFileById(steward, datasetId, loadedFiles.get(1).getFileId());

    BulkLoadFileResultModel file3 = loadedFiles.get(2);
    FileModel file3Model =
        dataRepoFixtures.getFileByName(steward, datasetId, file3.getTargetPath());
    assertThat("Test retrieve file by path", file3Model.getFileId(), equalTo(file3.getFileId()));

    FileModel file4Model =
        dataRepoFixtures.getFileById(steward, datasetId, loadedFiles.get(3).getFileId());

    // test the gcs file
    FileModel file5Model =
        dataRepoFixtures.getFileById(steward, datasetId, loadedFiles.get(4).getFileId());
    assertThat(
        "ensure that there is a non empty md5 present",
        file5Model.getChecksums().stream()
            .filter(
                c -> c.getType().equalsIgnoreCase("md5") && !StringUtils.isEmpty(c.getChecksum()))
            .toList(),
        hasSize(1));

    // ingest via control file
    String flightId = UUID.randomUUID().toString();
    String controlFileBlob = flightId + "/file-ingest-request.json";
    List<BulkLoadFileModel> bulkLoadFileModelList = new ArrayList<>();
    bulkLoadFileModelList.add(
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(azureBlobIOTestUtility.createSourcePath(sourceFileAzure))
            .targetPath(String.format("/%s/%s", flightId, "target.txt")));
    bulkLoadFileModelList.add(
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(azureBlobIOTestUtility.createSourcePath(sourceFileAzure))
            .targetPath(String.format("/%s/%s", flightId, "target2.txt")));

    String controlFileUrl =
        azureBlobIOTestUtility.uploadFileWithContents(
            controlFileBlob, TestUtils.readControlFile(bulkLoadFileModelList));

    String bulkLoadTag = Names.randomizeName("loadTag");
    BulkLoadRequestModel request =
        new BulkLoadRequestModel()
            .loadControlFile(controlFileUrl)
            .loadTag(bulkLoadTag)
            .profileId(profileId);
    BulkLoadResultModel bulkLoadResult = dataRepoFixtures.bulkLoad(steward, datasetId, request);
    assertThat("result", bulkLoadResult.getSucceededFiles(), equalTo(2));

    // Control file test - Look up the loaded files
    BulkLoadHistoryModelList controlFileLoadResults =
        dataRepoFixtures.getLoadHistory(steward, datasetId, bulkLoadTag, 0, 2);
    for (BulkLoadHistoryModel bulkFileEntry : controlFileLoadResults.getItems()) {
      assertNotNull(dataRepoFixtures.getFileById(steward, datasetId, bulkFileEntry.getFileId()));
    }

    DatasetModel datasetModel =
        dataRepoFixtures.getDataset(
            steward,
            datasetId,
            List.of(
                DatasetRequestAccessIncludeModel.ACCESS_INFORMATION,
                DatasetRequestAccessIncludeModel.SCHEMA));
    AccessInfoParquetModel datasetParquetAccessInfo =
        datasetModel.getAccessInformation().getParquet();

    DatasetSpecificationModel datasetSchema = datasetModel.getSchema();

    // dataset ingest
    // Ingest Metadata - 1 row in ARRAY mode
    String arrayIngestTableName = "person";
    String personIdField = "person_id";
    String personYearOfBirthField = "year_of_birth";
    Map<String, Integer> records = Map.of(personIdField, 1, personYearOfBirthField, 1980);
    testMetadataArrayIngest(arrayIngestTableName, records);
    @SuppressWarnings("unchecked")
    Map<Object, Object> firstPersonRow =
        (Map<Object, Object>)
            dataRepoFixtures
                .retrieveDatasetData(steward, datasetId, arrayIngestTableName, 0, 1, null)
                .getResult()
                .get(0);
    records.forEach((key, value) -> assertThat(firstPersonRow, hasEntry(key, value)));
    tableRowCount.put(arrayIngestTableName, 1);

    // Ingest Metadata - 1 row from JSON file
    String datasetIngestFlightId = UUID.randomUUID().toString();
    String datasetIngestControlFileBlob =
        datasetIngestFlightId + "/azure-domain-ingest-request.json";
    Map<String, Object> domainRowData =
        Map.ofEntries(
            Map.entry("domain_id", "1"),
            Map.entry("domain_name", "domain1"),
            Map.entry("domain_concept_id", 1),
            Map.entry("domain_array_tags_custom", List.of("tag1", "tag2")),
            Map.entry(
                "domain_files_custom_1", List.of(file1Model.getFileId(), file3Model.getFileId())),
            Map.entry("domain_files_custom_2", List.of(file2Model.getFileId())),
            Map.entry("domain_files_custom_3", file4Model.getFileId()));
    String ingestRequestPathJSON =
        azureBlobIOTestUtility.uploadFileWithContents(
            datasetIngestControlFileBlob,
            Objects.requireNonNull(TestUtils.mapToJson(domainRowData)));

    String jsonIngestTableName = "domain";
    IngestRequestModel ingestRequestJSON =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table(jsonIngestTableName)
            .path(ingestRequestPathJSON)
            .profileId(profileId)
            .loadTag(Names.randomizeName("test"));
    IngestResponseModel ingestResponseJSON =
        dataRepoFixtures.ingestJsonData(steward, datasetId, ingestRequestJSON);
    assertThat("1 row was ingested", ingestResponseJSON.getRowCount(), equalTo(1L));
    tableRowCount.put(jsonIngestTableName, 1);
    // assert correct row data was ingested into domain table
    dataRepoFixtures.assertDatasetTableCount(steward, datasetModel, "domain", 1);
    Object firstDomainRow =
        dataRepoFixtures
            .retrieveDatasetData(steward, datasetId, "domain", 0, 1, null)
            .getResult()
            .get(0);
    assertThat(
        "record looks as expected - domain_id",
        ((LinkedHashMap) firstDomainRow).get("domain_id").toString(),
        equalTo(domainRowData.get("domain_id")));
    assertThat(
        "record looks as expected - domain_files_custom_2 file id- value",
        ((ArrayList<String>) ((LinkedHashMap) firstDomainRow).get("domain_files_custom_2")).get(0),
        equalTo(file2Model.getFileId()));
    assertThat(
        "record looks as expected - domain_files_custom_3 file id- value",
        ((LinkedHashMap) firstDomainRow).get("domain_files_custom_3").toString(),
        equalTo(file4Model.getFileId()));

    // Ingest 2 rows from CSV
    String vocabTableName = "vocabulary";
    String csvDatasetIngestFlightId = UUID.randomUUID().toString();
    String csvDatasetIngestControlFileBlob =
        csvDatasetIngestFlightId + "/azure-vocab-ingest-request.csv";
    String ingestRequestPathCSV =
        azureBlobIOTestUtility.uploadFileWithContents(
            csvDatasetIngestControlFileBlob,
            String.format(
                // Note: the vocabulary_concept_id values are integers but have periods.  This is
                // to ensure that ingest properly handles truncating data in this case
                "vocabulary_id,vocabulary_name,vocabulary_reference,vocabulary_version,vocabulary_concept_id%n"
                    + "\"1\",\"vocab1\",\"%s\",\"v1\",1.0%n"
                    + "\"2\",\"vocab2\",\"%s\",\"v2\",2.0",
                file1Model.getFileId(), file3Model.getFileId()));
    IngestRequestModel ingestRequestCSV =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.CSV)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table(vocabTableName)
            .path(ingestRequestPathCSV)
            .profileId(profileId)
            .loadTag(Names.randomizeName("test"))
            .csvSkipLeadingRows(2);
    IngestResponseModel ingestResponseCSV =
        dataRepoFixtures.ingestJsonData(steward, datasetId, ingestRequestCSV);
    assertThat("2 row were ingested", ingestResponseCSV.getRowCount(), equalTo(2L));
    tableRowCount.put(vocabTableName, 2);

    // Dataset Schema Update & Ingest another row
    String newColumnName = "new_column";
    ColumnModel newColumn = DatasetFixtures.columnModel(newColumnName);
    DatasetSchemaUpdateModel updateModel =
        new DatasetSchemaUpdateModel()
            .description("Schema Additive Changes")
            .changes(
                new DatasetSchemaUpdateModelChanges()
                    .addColumns(
                        List.of(
                            DatasetFixtures.columnUpdateModel(vocabTableName, List.of(newColumn))))
                    .addTables(
                        List.of(
                            DatasetFixtures.tableModel("new_table", List.of("new_table_column")))));
    DatasetModel response = dataRepoFixtures.updateSchema(steward(), datasetId, updateModel);
    assertThat(
        "The new table is in the update response",
        response.getSchema().getTables().stream()
            .filter(tableModel -> tableModel.getName().equals("new_table"))
            .findFirst()
            .isPresent());
    Map<String, String> vocab_entry2 =
        Map.of(
            "vocabulary_id",
            "3",
            "vocabulary_name",
            "vocab_3",
            "vocabulary_version",
            "v3",
            "vocabulary_concept_id",
            "3.0",
            newColumnName,
            "new_value");
    testMetadataArrayIngest(vocabTableName, vocab_entry2);
    tableRowCount.put(vocabTableName, 3);

    // assert correct data returns from view data endpoint
    dataRepoFixtures.assertDatasetTableCount(
        steward, datasetModel, vocabTableName, tableRowCount.get(vocabTableName));
    List<Object> vocabRows =
        dataRepoFixtures
            .retrieveDatasetData(
                steward,
                datasetId,
                vocabTableName,
                0,
                tableRowCount.get(vocabTableName),
                null,
                "vocabulary_id",
                SqlSortDirectionAscDefault.ASC)
            .getResult();
    assertThat(
        "record looks as expected - vocabulary_id",
        ((LinkedHashMap) vocabRows.get(0)).get("vocabulary_id").toString(),
        equalTo("1"));
    assertThat(
        "record looks as expected - vocabulary_id",
        ((LinkedHashMap) vocabRows.get(1)).get("vocabulary_id").toString(),
        equalTo("2"));
    assertThat(
        "record looks as expected - new column",
        ((LinkedHashMap) vocabRows.get(2)).get(newColumnName).toString(),
        equalTo("new_value"));
    List<String> vocabList =
        dataRepoFixtures.retrieveColumnTextValues(
            steward, datasetId, "vocabulary", "vocabulary_id");
    assertThat(
        "Vocabulary table contains correct vocabulary_ids",
        vocabList,
        containsInAnyOrder("1", "2", "3"));
    ColumnStatisticsIntModel intModel =
        dataRepoFixtures.retrieveColumnIntStats(
            steward, datasetId, "vocabulary", "vocabulary_concept_id", null);
    assertThat("Correct max values in vocabulary_concept_id", intModel.getMaxValue(), equalTo(3));
    assertThat("Correct min values in vocabulary_concept_id", intModel.getMinValue(), equalTo(1));
    List<Object> flippedVocabRows =
        dataRepoFixtures
            .retrieveDatasetData(
                steward,
                datasetId,
                vocabTableName,
                0,
                tableRowCount.get(vocabTableName),
                null,
                "vocabulary_id",
                SqlSortDirectionAscDefault.DESC)
            .getResult();
    assertThat(
        "correct vocabulary_id returned",
        ((LinkedHashMap) flippedVocabRows.get(0)).get("vocabulary_id").toString(),
        equalTo("3"));
    String qualifiedVocabTableName = String.format("%s.%s", datasetModel.getName(), vocabTableName);
    DatasetDataModel filteredVocabRows =
        dataRepoFixtures.retrieveDatasetData(
            steward,
            datasetId,
            vocabTableName,
            0,
            tableRowCount.get(vocabTableName),
            "vocabulary_id = '1'");
    assertThat(
        "correct number of rows returned after filtering",
        filteredVocabRows.getResult(),
        hasSize(1));
    assertThat("filter row count is correct", filteredVocabRows.getFilteredRowCount(), equalTo(1));
    assertThat(
        "total row count is correct",
        filteredVocabRows.getTotalRowCount(),
        equalTo(tableRowCount.get(vocabTableName)));
    assertThat(
        "Correct row is returned",
        ((LinkedHashMap) filteredVocabRows.getResult().get(0)).get("vocabulary_id").toString(),
        equalTo("1"));

    // test handling of empty dataset table
    dataRepoFixtures.assertDatasetTableCount(steward, datasetModel, "concept", 0);
    dataRepoFixtures.assertDatasetTableCount(steward, datasetModel, "new_table", 0);

    // test handling of not-empty dataset table filtered to empty
    DatasetDataModel emptyFilteredVocabRows =
        dataRepoFixtures.retrieveDatasetData(
            steward,
            datasetId,
            vocabTableName,
            0,
            tableRowCount.get(vocabTableName),
            "vocabulary_id = 'xy'");
    assertThat(
        "correct number of rows returned after filtering",
        emptyFilteredVocabRows.getResult(),
        hasSize(0));
    assertThat(
        "filter row count is correct", emptyFilteredVocabRows.getFilteredRowCount(), equalTo(0));
    assertThat(
        "total row count is correct",
        emptyFilteredVocabRows.getTotalRowCount(),
        equalTo(tableRowCount.get(vocabTableName)));

    // Create snapshot request for snapshot by row id
    String datasetParquetUrl =
        datasetParquetAccessInfo.getUrl() + "?" + datasetParquetAccessInfo.getSasToken();
    TestUtils.verifyHttpAccess(datasetParquetUrl, Map.of());
    verifySignedUrl(datasetParquetUrl, steward, "rl");

    SnapshotRequestModel snapshotByRowIdModel = new SnapshotRequestModel();
    snapshotByRowIdModel.setName("row_id_test");
    snapshotByRowIdModel.setDescription("snapshot by row id test");

    SnapshotRequestContentsModel contentsModel = new SnapshotRequestContentsModel();
    contentsModel.setDatasetName(summaryModel.getName());
    contentsModel.setMode(SnapshotRequestContentsModel.ModeEnum.BYROWID);

    SnapshotRequestRowIdModel snapshotRequestRowIdModel = new SnapshotRequestRowIdModel();

    for (AccessInfoParquetModelTable table : datasetParquetAccessInfo.getTables()) {
      if (tableRowCount.containsKey(table.getName())) {
        String tableUrl = table.getUrl() + "?" + table.getSasToken();
        TestUtils.verifyHttpAccess(tableUrl, Map.of());
        verifySignedUrl(tableUrl, steward, "rl");

        SnapshotRequestRowIdTableModel tableModel = new SnapshotRequestRowIdTableModel();
        tableModel.setTableName(table.getName());
        tableModel.setColumns(
            datasetSchema.getTables().stream()
                .filter(t -> t.getName().equals(table.getName()))
                .flatMap(t -> t.getColumns().stream().map(c -> c.getName()))
                .toList());
        tableModel.setRowIds(
            dataRepoFixtures
                .getRowIds(
                    steward, datasetModel, table.getName(), tableRowCount.get(table.getName()))
                .stream()
                .map(id -> UUID.fromString(id))
                .toList());
        snapshotRequestRowIdModel.addTablesItem(tableModel);
      }
    }

    contentsModel.setRowIdSpec(snapshotRequestRowIdModel);
    snapshotByRowIdModel.setContents(List.of(contentsModel));

    // -------- Create Snapshot by full view ------

    // create Sam Group
    String groupName = UUID.randomUUID().toString();
    samFixtures.addGroup(steward, groupName);
    dac = groupName;

    SnapshotRequestModel requestModelAll =
        jsonLoader.loadObject("ingest-test-snapshot-fullviews.json", SnapshotRequestModel.class);
    requestModelAll.getContents().get(0).datasetName(summaryModel.getName());
    requestModelAll.dataAccessControlGroups(List.of(groupName));

    SnapshotSummaryModel snapshotSummaryAll =
        dataRepoFixtures.createSnapshotWithRequest(
            steward, summaryModel.getName(), profileId, requestModelAll);
    UUID snapshotByFullViewId = snapshotSummaryAll.getId();
    snapshotIds.add(snapshotByFullViewId);
    recordStorageAccount(steward, CollectionType.SNAPSHOT, snapshotByFullViewId);
    assertThat("Snapshot exists", snapshotSummaryAll.getName(), equalTo(requestModelAll.getName()));
    List<String> dacs =
        samFixtures.getAuthDomainForResource(
            steward,
            IamResourceType.DATASNAPSHOT.getSamResourceName(),
            String.valueOf(snapshotByFullViewId));
    assertThat("Snapshot has the expected DAC", dacs, containsInAnyOrder(groupName));

    // Ensure that export works
    DataRepoResponse<SnapshotExportResponseModel> snapshotExport =
        dataRepoFixtures.exportSnapshotLog(steward, snapshotByFullViewId, false, false, true);

    assertThat(
        "snapshotExport is present", snapshotExport.getResponseObject().isPresent(), is(true));
    // Verify that the manifest is accessible
    SnapshotExportResponseModelFormatParquet manifest =
        snapshotExport.getResponseObject().get().getFormat().getParquet();
    verifyUrlIsAccessible(manifest.getManifest());
    // Verify that all files referenced in manifest are accessible
    manifest.getLocation().getTables().stream()
        .flatMap(t -> t.getPaths().stream())
        .forEach(this::verifyUrlIsAccessible);

    // Read the ingested metadata
    SnapshotModel snapshotAll =
        dataRepoFixtures.getSnapshot(
            steward,
            snapshotByFullViewId,
            List.of(SnapshotRetrieveIncludeModel.ACCESS_INFORMATION));
    AccessInfoParquetModel snapshotParquetAccessInfo =
        snapshotAll.getAccessInformation().getParquet();

    String snapshotParquetUrl =
        snapshotParquetAccessInfo.getUrl() + "?" + snapshotParquetAccessInfo.getSasToken();
    TestUtils.verifyHttpAccess(snapshotParquetUrl, Map.of());
    verifySignedUrl(snapshotParquetUrl, steward, "rl");

    // Vocabulary Table
    dataRepoFixtures.assertSnapshotTableCount(
        steward, snapshotAll, vocabTableName, tableRowCount.get(vocabTableName));
    List<Object> vocabSnapshotRows =
        dataRepoFixtures
            .retrieveSnapshotPreviewById(
                steward,
                snapshotAll.getId(),
                vocabTableName,
                0,
                tableRowCount.get(vocabTableName),
                null,
                "vocabulary_id")
            .getResult();
    List<String> drsIds =
        vocabSnapshotRows.stream()
            .filter(r -> ((LinkedHashMap) r).get("vocabulary_reference") != null)
            .map(r -> ((LinkedHashMap) r).get("vocabulary_reference").toString())
            .toList();

    Object firstVocabRow = vocabSnapshotRows.get(0);
    assertThat(
        "record looks as expected - vocabulary_id",
        ((LinkedHashMap) firstVocabRow).get("vocabulary_id").toString(),
        equalTo("1"));
    Object secondVocabRow = vocabSnapshotRows.get(1);
    assertThat(
        "record looks as expected - vocabulary_id",
        ((LinkedHashMap) secondVocabRow).get("vocabulary_id").toString(),
        equalTo("2"));

    // Test filtering results from snapshot preview endpoint and check row counts
    // filtered so that filtered row count > 0, but not equal to total row count
    SnapshotPreviewModel filteredVocabSnapshotRows =
        dataRepoFixtures.retrieveSnapshotPreviewById(
            steward,
            snapshotAll.getId(),
            vocabTableName,
            0,
            tableRowCount.get(vocabTableName),
            "vocabulary_id = '1'",
            "vocabulary_id");
    assertThat(
        "correct number of rows returned after filtering",
        filteredVocabSnapshotRows.getResult(),
        hasSize(1));
    assertThat(
        "filter row count is correct", filteredVocabSnapshotRows.getFilteredRowCount(), equalTo(1));
    assertThat(
        "total row count is correct",
        filteredVocabSnapshotRows.getTotalRowCount(),
        equalTo(tableRowCount.get(vocabTableName)));
    assertThat(
        "Correct row is returned",
        ((LinkedHashMap) filteredVocabRows.getResult().get(0)).get("vocabulary_id").toString(),
        equalTo("1"));

    // test handling of empty snapshot table
    dataRepoFixtures.assertSnapshotTableCount(steward, snapshotAll, "concept", 0);

    // test handling of not-empty snapshot table filtered to empty
    SnapshotPreviewModel emptyFilteredVocabSnapshotRows =
        dataRepoFixtures.retrieveSnapshotPreviewById(
            steward,
            snapshotAll.getId(),
            vocabTableName,
            0,
            tableRowCount.get(vocabTableName),
            "vocabulary_id = 'xy'",
            "vocabulary_id");
    assertThat(
        "correct number of rows returned after filtering",
        emptyFilteredVocabSnapshotRows.getResult(),
        hasSize(0));
    assertThat(
        "filter row count is correct",
        emptyFilteredVocabSnapshotRows.getFilteredRowCount(),
        equalTo(0));
    assertThat(
        "total row count is correct",
        emptyFilteredVocabSnapshotRows.getTotalRowCount(),
        equalTo(tableRowCount.get(vocabTableName)));

    // Domain Table
    dataRepoFixtures.assertSnapshotTableCount(steward, snapshotAll, "domain", 1);
    Object firstSnapshotDomainRow =
        dataRepoFixtures.retrieveFirstResultSnapshotPreviewById(
            steward, snapshotAll.getId(), "domain", 0, 1, null);
    assertThat(
        "record looks as expected - domain_id",
        ((LinkedHashMap) firstSnapshotDomainRow).get("domain_id").toString(),
        equalTo(domainRowData.get("domain_id")));
    assertThat(
        "record looks as expected - domain_name",
        ((LinkedHashMap) firstSnapshotDomainRow).get("domain_name").toString(),
        equalTo(domainRowData.get("domain_name")));
    assertThat(
        "record looks as expected - domain_concept_id",
        ((LinkedHashMap) firstSnapshotDomainRow).get("domain_concept_id").toString(),
        equalTo(domainRowData.get("domain_concept_id").toString()));
    assertThat(
        "record looks as expected - domain_array_tags_custom",
        ((LinkedHashMap) firstSnapshotDomainRow).get("domain_array_tags_custom").toString(),
        equalTo("[tag1, tag2]"));
    assertThat(
        "record looks as expected - domain_files_custom_1 drs ids",
        (ArrayList<String>) ((LinkedHashMap) firstSnapshotDomainRow).get("domain_files_custom_1"),
        containsInAnyOrder(drsIds.toArray()));
    List<String> embeddedDrsIds2 =
        (ArrayList<String>) ((LinkedHashMap) firstSnapshotDomainRow).get("domain_files_custom_2");
    assertThat(
        "record looks as expected - domain_files_custom_2 drs ids - size",
        embeddedDrsIds2,
        hasSize(1));
    assertThat(
        "record looks as expected - domain_files_custom_2 drs ids - value",
        DrsIdService.fromUri(embeddedDrsIds2.get(0)).toDrsObjectId(),
        equalTo(String.format("v1_%s_%s", snapshotByFullViewId, file2Model.getFileId())));
    assertThat(
        "record looks as expected - domain_files_custom_3 drs id",
        DrsIdService.fromUri(
                ((LinkedHashMap) firstSnapshotDomainRow).get("domain_files_custom_3").toString())
            .toDrsObjectId(),
        equalTo(String.format("v1_%s_%s", snapshotByFullViewId, file4Model.getFileId())));

    // Assert that 2 drs ids were loaded
    assertThat("2 drs ids are present", drsIds, hasSize(2));
    // Ensure that all DRS can be parsed
    List<String> drsObjectIds =
        drsIds.stream().map(DrsIdService::fromUri).map(DrsId::toDrsObjectId).toList();

    String fileId = result.getLoadFileResults().get(0).getFileId();
    String filePath = result.getLoadFileResults().get(0).getTargetPath();

    // Do a Drs lookup
    String drsId = String.format("v1_%s_%s", snapshotByFullViewId, fileId);
    assertThat("Expected Drs object Id exists", drsObjectIds.contains(drsId));
    DRSObject drsObject = dataRepoFixtures.drsGetObject(steward, drsId);
    assertThat("DRS object has single access method", drsObject.getAccessMethods(), hasSize(1));
    assertThat(
        "DRS object has HTTPS",
        drsObject.getAccessMethods().get(0).getType(),
        equalTo(DRSAccessMethod.TypeEnum.HTTPS));
    assertThat(
        "DRS object has access id",
        drsObject.getAccessMethods().get(0).getAccessId(),
        equalTo("az-centralus"));
    // Make sure we can read the drs object
    DrsResponse<DRSAccessURL> access =
        dataRepoFixtures.getObjectAccessUrl(steward, drsId, "az-centralus");
    assertThat("Returns DRS access", access.getResponseObject().isPresent(), is(true));
    String signedUrl = access.getResponseObject().get().getUrl();

    TestUtils.verifyHttpAccess(signedUrl, Map.of());
    verifySignedUrl(signedUrl, steward, "r");

    // -------- Create snapshot by Query ---------
    // Build snapshot request for snapshot by query
    SnapshotRequestModel snapshotByQueryModel = new SnapshotRequestModel();
    snapshotByQueryModel.setName("query_test");
    snapshotByQueryModel.setDescription("snapshot by query test");

    SnapshotRequestContentsModel snapshotRequestByQueryContentsModel =
        new SnapshotRequestContentsModel();
    snapshotRequestByQueryContentsModel.setDatasetName(summaryModel.getName());

    snapshotRequestByQueryContentsModel.setMode(SnapshotRequestContentsModel.ModeEnum.BYQUERY);

    SnapshotRequestQueryModel snapshotRequestQueryModel = new SnapshotRequestQueryModel();
    snapshotRequestQueryModel.setAssetName("vocab_single");
    snapshotRequestQueryModel.setQuery(
        "Select "
            + qualifiedVocabTableName
            + ".datarepo_row_id FROM "
            + qualifiedVocabTableName
            + " WHERE "
            + qualifiedVocabTableName
            + ".vocabulary_id = '1'");
    snapshotRequestByQueryContentsModel.setQuerySpec(snapshotRequestQueryModel);
    snapshotByQueryModel.setContents(List.of(snapshotRequestByQueryContentsModel));

    SnapshotSummaryModel snapshotSummaryByQuery =
        dataRepoFixtures.createSnapshotWithRequest(
            steward, summaryModel.getName(), profileId, snapshotByQueryModel);
    UUID snapshotByQueryId = snapshotSummaryByQuery.getId();
    snapshotIds.add(snapshotByQueryId);
    recordStorageAccount(steward, CollectionType.SNAPSHOT, snapshotByQueryId);
    assertThat(
        "Snapshot by query exists",
        snapshotSummaryByQuery.getName(),
        equalTo(snapshotByQueryModel.getName()));

    // Read the snapshot
    AccessInfoParquetModel snapshotByQueryParquetAccessInfo =
        dataRepoFixtures
            .getSnapshot(
                steward,
                snapshotByQueryId,
                List.of(SnapshotRetrieveIncludeModel.ACCESS_INFORMATION))
            .getAccessInformation()
            .getParquet();

    String snapshotByQueryParquetUrl =
        snapshotByQueryParquetAccessInfo.getUrl()
            + "?"
            + snapshotByQueryParquetAccessInfo.getSasToken();
    TestUtils.verifyHttpAccess(snapshotByQueryParquetUrl, Map.of());
    verifySignedUrl(snapshotByQueryParquetUrl, steward, "rl");

    for (AccessInfoParquetModelTable table : snapshotByQueryParquetAccessInfo.getTables()) {
      if (vocabTableName.equals(table.getName())) {
        String tableUrl = table.getUrl() + "?" + table.getSasToken();
        TestUtils.verifyHttpAccess(tableUrl, Map.of());
        verifySignedUrl(tableUrl, steward, "rl");
      }
    }

    // -------- Create snapshot by Asset ---------
    // Build snapshot request for snapshot by asset
    SnapshotRequestModel snapshotByAssetModel = new SnapshotRequestModel();
    snapshotByAssetModel.setName("asset_test");
    snapshotByAssetModel.setDescription("snapshot by asset test");

    SnapshotRequestContentsModel snapshotRequestByAssetContentsModel =
        new SnapshotRequestContentsModel();
    snapshotRequestByAssetContentsModel.setDatasetName(summaryModel.getName());

    snapshotRequestByAssetContentsModel.setMode(SnapshotRequestContentsModel.ModeEnum.BYASSET);

    SnapshotRequestAssetModel snapshotRequestAssetModel = new SnapshotRequestAssetModel();
    snapshotRequestAssetModel.setAssetName("vocab_single");
    snapshotRequestAssetModel.setRootValues(List.of("1"));
    snapshotRequestByAssetContentsModel.setAssetSpec(snapshotRequestAssetModel);
    snapshotByAssetModel.setContents(List.of(snapshotRequestByAssetContentsModel));

    SnapshotSummaryModel snapshotSummaryByAsset =
        dataRepoFixtures.createSnapshotWithRequest(
            steward, summaryModel.getName(), profileId, snapshotByAssetModel);
    UUID snapshotByAssetId = snapshotSummaryByAsset.getId();
    snapshotIds.add(snapshotByAssetId);
    recordStorageAccount(steward, CollectionType.SNAPSHOT, snapshotByAssetId);
    assertThat(
        "Snapshot by asset exists",
        snapshotSummaryByAsset.getName(),
        equalTo(snapshotByAssetModel.getName()));

    // Read the snapshot
    AccessInfoParquetModel snapshotByAssetParquetAccessInfo =
        dataRepoFixtures
            .getSnapshot(
                steward,
                snapshotByAssetId,
                List.of(SnapshotRetrieveIncludeModel.ACCESS_INFORMATION))
            .getAccessInformation()
            .getParquet();

    String snapshotByAssetParquetUrl =
        snapshotByAssetParquetAccessInfo.getUrl()
            + "?"
            + snapshotByAssetParquetAccessInfo.getSasToken();
    TestUtils.verifyHttpAccess(snapshotByAssetParquetUrl, Map.of());
    verifySignedUrl(snapshotByAssetParquetUrl, steward, "rl");

    for (AccessInfoParquetModelTable table : snapshotByAssetParquetAccessInfo.getTables()) {
      if (vocabTableName.equals(table.getName())) {
        String tableUrl = table.getUrl() + "?" + table.getSasToken();
        TestUtils.verifyHttpAccess(tableUrl, Map.of());
        verifySignedUrl(tableUrl, steward, "rl");
      }
    }

    // -------- Create snapshot by row id --------
    SnapshotSummaryModel snapshotSummaryByRowId =
        dataRepoFixtures.createSnapshotWithRequest(
            steward, summaryModel.getName(), profileId, snapshotByRowIdModel);
    UUID snapshotByRowId = snapshotSummaryByRowId.getId();
    snapshotIds.add(snapshotByRowId);
    recordStorageAccount(steward, CollectionType.SNAPSHOT, snapshotByRowId);
    assertThat(
        "Snapshot exists",
        snapshotSummaryByRowId.getName(),
        equalTo(snapshotByRowIdModel.getName()));

    // Read the ingested metadata
    AccessInfoParquetModel snapshotByRowIdParquetAccessInfo =
        dataRepoFixtures
            .getSnapshot(
                steward, snapshotByRowId, List.of(SnapshotRetrieveIncludeModel.ACCESS_INFORMATION))
            .getAccessInformation()
            .getParquet();

    String snapshotByRowIdParquetUrl =
        snapshotByRowIdParquetAccessInfo.getUrl()
            + "?"
            + snapshotByRowIdParquetAccessInfo.getSasToken();
    TestUtils.verifyHttpAccess(snapshotByRowIdParquetUrl, Map.of());
    verifySignedUrl(snapshotByRowIdParquetUrl, steward, "rl");

    for (AccessInfoParquetModelTable table : snapshotByRowIdParquetAccessInfo.getTables()) {
      // only run check for tables that have ingested data
      if (tableRowCount.containsKey(table.getName())) {
        String tableUrl = table.getUrl() + "?" + table.getSasToken();
        TestUtils.verifyHttpAccess(tableUrl, Map.of());
        verifySignedUrl(tableUrl, steward, "rl");
      }
    }

    // Do a Drs lookup
    String drsIdByRowId = String.format("v1_%s_%s", snapshotByRowId, fileId);
    DRSObject drsObjectByRowId = dataRepoFixtures.drsGetObject(steward, drsIdByRowId);
    assertThat(
        "DRS object has single access method",
        drsObjectByRowId.getAccessMethods().size(),
        equalTo(1));
    assertThat(
        "DRS object has HTTPS",
        drsObjectByRowId.getAccessMethods().get(0).getType(),
        equalTo(DRSAccessMethod.TypeEnum.HTTPS));
    assertThat(
        "DRS object has access id",
        drsObjectByRowId.getAccessMethods().get(0).getAccessId(),
        equalTo("az-centralus"));
    // Make sure we can read the drs object
    DrsResponse<DRSAccessURL> accessForByRowId =
        dataRepoFixtures.getObjectAccessUrl(steward, drsIdByRowId, "az-centralus");
    assertThat("Returns DRS access", accessForByRowId.getResponseObject().isPresent(), is(true));
    String signedUrlForByRowId = accessForByRowId.getResponseObject().get().getUrl();

    TestUtils.verifyHttpAccess(signedUrlForByRowId, Map.of());
    verifySignedUrl(signedUrlForByRowId, steward, "r");

    // Make sure that only 1 storage account was created
    // record the storage account
    assertThat("only one storage account exists", storageAccounts, hasSize(1));

    // Delete dataset should fail
    dataRepoFixtures.deleteDatasetShouldFail(steward, datasetId);

    // Delete snapshot
    dataRepoFixtures.deleteSnapshot(steward, snapshotByFullViewId);
    snapshotIds.remove(snapshotByFullViewId);
    dataRepoFixtures.deleteSnapshot(steward, snapshotByRowId);
    snapshotIds.remove(snapshotByRowId);
    dataRepoFixtures.deleteSnapshot(steward, snapshotByAssetId);
    snapshotIds.remove(snapshotByAssetId);
    dataRepoFixtures.deleteSnapshot(steward, snapshotByQueryId);
    snapshotIds.remove(snapshotByQueryId);

    dataRepoFixtures.assertFailToGetSnapshot(steward, snapshotByFullViewId);
    dataRepoFixtures.assertFailToGetSnapshot(steward, snapshotByRowId);

    // Delete the file we just ingested
    dataRepoFixtures.deleteFile(steward, datasetId, fileId);

    assertThat(
        "file is gone",
        dataRepoFixtures.getFileByIdRaw(steward, datasetId, fileId).getStatusCode(),
        equalTo(HttpStatus.NOT_FOUND));

    assertThat(
        "file is gone",
        dataRepoFixtures.getFileByNameRaw(steward, datasetId, filePath).getStatusCode(),
        equalTo(HttpStatus.NOT_FOUND));

    // Delete dataset should now succeed
    dataRepoFixtures.deleteDataset(steward, datasetId);
    datasetId = null;

    // Make sure that any failure in tearing down is presented as a test failure
    azureBlobIOTestUtility.teardown();
  }

  @Test
  public void testDatasetFileIngestLoadHistory() throws Exception {
    String blobName = "myBlob";
    long fileSize = MIB / 10;
    String sourceFile = azureBlobIOTestUtility.uploadSourceFile(blobName, fileSize);
    DatasetSummaryModel summaryModel =
        dataRepoFixtures.createDataset(
            steward, profileId, "omop/it-dataset-omop.json", CloudPlatform.AZURE);
    datasetId = summaryModel.getId();

    BulkLoadFileModel fileLoadModel =
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(azureBlobIOTestUtility.createSourcePath(sourceFile))
            .targetPath("/test/target.txt");
    BulkLoadFileModel fileLoadModelSas =
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(
                azureBlobIOTestUtility.createSourceSignedPath(
                    sourceFile, getSourceStorageAccountPrimarySharedKey()))
            .targetPath("/test/targetSas.txt");
    BulkLoadArrayResultModel bulkLoadResult1 =
        dataRepoFixtures.bulkLoadArray(
            steward,
            datasetId,
            new BulkLoadArrayRequestModel()
                .profileId(summaryModel.getDefaultProfileId())
                .loadTag("loadTag")
                .addLoadArrayItem(fileLoadModel)
                .addLoadArrayItem(fileLoadModelSas));

    BulkLoadFileModel fileLoadModel2 =
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(azureBlobIOTestUtility.createSourcePath(sourceFile))
            .targetPath("/test/target2.txt");
    BulkLoadFileModel fileLoadModelSas2 =
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(
                azureBlobIOTestUtility.createSourceSignedPath(
                    sourceFile, getSourceStorageAccountPrimarySharedKey()))
            .targetPath("/test/targetSas2.txt");

    BulkLoadArrayResultModel bulkLoadResult2 =
        dataRepoFixtures.bulkLoadArray(
            steward,
            datasetId,
            new BulkLoadArrayRequestModel()
                .profileId(summaryModel.getDefaultProfileId())
                .loadTag("loadTag")
                .addLoadArrayItem(fileLoadModel2)
                .addLoadArrayItem(fileLoadModelSas2));

    BulkLoadFileModel fileLoadModel3 =
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(azureBlobIOTestUtility.createSourcePath(sourceFile))
            .targetPath("/test/target3.txt");
    BulkLoadFileModel fileLoadModelSas3 =
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(
                azureBlobIOTestUtility.createSourceSignedPath(
                    sourceFile, getSourceStorageAccountPrimarySharedKey()))
            .targetPath("/test/targetSas3.txt");
    dataRepoFixtures.bulkLoadArray(
        steward,
        datasetId,
        new BulkLoadArrayRequestModel()
            .profileId(summaryModel.getDefaultProfileId())
            .loadTag("differentLoadTag")
            .addLoadArrayItem(fileLoadModel3)
            .addLoadArrayItem(fileLoadModelSas3));

    var loadHistoryList1 = dataRepoFixtures.getLoadHistory(steward, datasetId, "loadTag", 0, 2);
    var loadHistoryList2 = dataRepoFixtures.getLoadHistory(steward, datasetId, "loadTag", 2, 10);
    var loadHistoryList1and2 =
        dataRepoFixtures.getLoadHistory(steward, datasetId, "loadTag", 0, 10);
    var loadHistoryList3 =
        dataRepoFixtures.getLoadHistory(steward, datasetId, "differentLoadTag", 0, 10);
    var loaded1and2 =
        Stream.concat(
                bulkLoadResult1.getLoadFileResults().stream(),
                bulkLoadResult2.getLoadFileResults().stream())
            .collect(Collectors.toSet());

    var loadHistory1and2Models =
        Stream.concat(loadHistoryList1.getItems().stream(), loadHistoryList2.getItems().stream());

    assertThat("limited load history is the correct size", loadHistoryList1.getTotal(), equalTo(2));
    assertThat("offset load history is the correct size", loadHistoryList2.getTotal(), equalTo(2));
    assertThat(
        "all load history for load tag is returned", loadHistoryList1and2.getTotal(), equalTo(4));
    assertThat(
        "getting load history has the same items as response from bulk file load",
        loadHistory1and2Models
            .map(TestUtils::toBulkLoadFileResultModel)
            .collect(Collectors.toSet()),
        equalTo(loaded1and2));
    assertThat(
        "load counts under different load tags are returned separately",
        loadHistoryList3.getTotal(),
        equalTo(2));
    for (var loadHistoryModel : loadHistoryList3.getItems()) {
      assertThat(
          "models from different load tags are returned in different requests",
          loadHistoryModel,
          not(in(loadHistoryList1and2.getItems())));
    }

    // Make sure that any failure in tearing down is presented as a test failure
    azureBlobIOTestUtility.teardown();
  }

  @Test
  public void testDatasetFileRefValidation() throws Exception {
    DatasetSummaryModel summaryModel =
        dataRepoFixtures.createDataset(
            steward, profileId, "dataset-ingest-azure-fileref.json", CloudPlatform.AZURE);
    datasetId = summaryModel.getId();
    recordStorageAccount(steward, CollectionType.DATASET, datasetId);

    String noFilesContents =
        "sample_name,data_type,vcf_file_ref,vcf_index_file_ref\n"
            + String.format("NA12878_none,none,%s,%s", UUID.randomUUID(), UUID.randomUUID());
    String noFilesControlFile =
        azureBlobIOTestUtility.uploadFileWithContents(
            "dataset-files-ingest-fail.csv", noFilesContents);

    IngestRequestModel noFilesIngestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.CSV)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table("sample_vcf")
            .path(noFilesControlFile)
            .profileId(profileId)
            .loadTag(Names.randomizeName("test2"))
            .csvSkipLeadingRows(2);

    DataRepoResponse<IngestResponseModel> noFilesIngestResponse =
        dataRepoFixtures.ingestJsonDataRaw(steward, datasetId, noFilesIngestRequest);

    assertThat(
        "No files yet loaded doesn't result in an NPE",
        noFilesIngestResponse.getErrorObject().get().getMessage(),
        equalTo("Invalid file ids found during ingest (2 returned in details)"));

    String loadTag = UUID.randomUUID().toString();
    var arrayRequestModel =
        new BulkLoadArrayRequestModel()
            .profileId(summaryModel.getDefaultProfileId())
            .loadTag(loadTag);

    long fileSize = MIB / 10;
    Stream.of(
            "NA12878_PLUMBING_exome.g.vcf.gz",
            "NA12878_PLUMBING_exome.g.vcf.gz.tbi",
            "NA12878_PLUMBING_wgs.g.vcf.gz",
            "NA12878_PLUMBING_wgs.g.vcf.gz.tbi")
        .map(
            name -> {
              String sourceFile = azureBlobIOTestUtility.uploadSourceFile(name, fileSize);
              return new BulkLoadFileModel()
                  .sourcePath(azureBlobIOTestUtility.createSourcePath(sourceFile))
                  .targetPath("/vcfs/downsampled/" + name)
                  .description("Test file for " + name)
                  .mimeType("text/plain");
            })
        .forEach(arrayRequestModel::addLoadArrayItem);

    var bulkLoadArrayResultModel =
        dataRepoFixtures.bulkLoadArray(steward, datasetId, arrayRequestModel);

    var resultModels =
        bulkLoadArrayResultModel.getLoadFileResults().stream()
            .collect(
                Collectors.toMap(
                    m -> m.getTargetPath().replaceAll("/vcfs/downsampled/NA12878_PLUMBING_", ""),
                    Function.identity()));
    var exomeVcf = resultModels.get("exome.g.vcf.gz");
    var exomeVcfIndex = resultModels.get("exome.g.vcf.gz.tbi");
    var wgsVcf = resultModels.get("wgs.g.vcf.gz");
    var wgsVcfIndex = resultModels.get("wgs.g.vcf.gz.tbi");

    var datasetMetadata =
        Files.readString(
            ResourceUtils.getFile("classpath:dataset-ingest-combined-metadata-only.csv").toPath());
    var metadataWithFileIds =
        datasetMetadata
            .replaceFirst("EXOME_VCF_FILE_REF", exomeVcf.getFileId())
            .replaceFirst("EXOME_VCF_INDEX_FILE_REF", exomeVcfIndex.getFileId())
            .replaceFirst("WGS_VCF_FILE_REF", wgsVcf.getFileId())
            .replaceFirst("WGS_VCF_INDEX_FILE_REF", wgsVcfIndex.getFileId());

    String controlFile =
        azureBlobIOTestUtility.uploadFileWithContents(
            "dataset-files-ingest.csv", metadataWithFileIds);
    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.CSV)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table("sample_vcf")
            .path(controlFile)
            .profileId(profileId)
            .loadTag(Names.randomizeName("test"))
            .csvSkipLeadingRows(2);

    IngestResponseModel ingestResponseJson =
        dataRepoFixtures.ingestJsonData(steward, datasetId, ingestRequest);

    assertThat(
        "there are two successful ingest rows", ingestResponseJson.getRowCount(), equalTo(2L));
    assertThat("there were no bad ingest rows", ingestResponseJson.getBadRowCount(), equalTo(0L));

    IngestRequestModel failingIngestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.CSV)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table("sample_vcf")
            .path(noFilesControlFile)
            .profileId(profileId)
            .loadTag(Names.randomizeName("test2"))
            .csvSkipLeadingRows(2);

    DataRepoResponse<IngestResponseModel> failingIngestResponse =
        dataRepoFixtures.ingestJsonDataRaw(steward, datasetId, failingIngestRequest);

    assertThat(
        "Failing fileIds return an error",
        failingIngestResponse.getErrorObject().isPresent(),
        is(true));

    assertThat(
        "2 invalid ids were returned",
        failingIngestResponse.getErrorObject().get().getErrorDetail(),
        hasSize(2));

    // Make sure that any failure in tearing down is presented as a test failure
    azureBlobIOTestUtility.teardown();
  }

  @Test
  public void testRequiredColumnsIngest() throws Exception {
    DatasetSummaryModel summaryModel =
        dataRepoFixtures.createDataset(
            steward,
            profileId,
            "dataset-ingest-combined-azure-required-columns.json",
            CloudPlatform.AZURE);
    datasetId = summaryModel.getId();
    recordStorageAccount(steward, CollectionType.DATASET, datasetId);

    String controlFileContents;
    try (var resourceStream =
        this.getClass().getResourceAsStream("/dataset-ingest-combined-control-azure.json")) {
      controlFileContents = new String(resourceStream.readAllBytes(), StandardCharsets.UTF_8);
    }

    String controlFile =
        azureBlobIOTestUtility.uploadFileWithContents(
            "dataset-files-ingest-combined.json", controlFileContents);

    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table("sample_vcf")
            .profileId(profileId)
            .path(controlFile)
            .format(IngestRequestModel.FormatEnum.JSON)
            .loadTag(Names.randomizeName("azureCombinedIngestTest"));

    DataRepoResponse<IngestResponseModel> ingestResponseFail =
        dataRepoFixtures.ingestJsonDataRaw(steward, datasetId, ingestRequest);

    assertThat(
        "ingesting null values into required columns results in failure",
        ingestResponseFail.getErrorObject().isPresent());

    ErrorModel errorResponse = ingestResponseFail.getErrorObject().orElseThrow();

    assertThat(
        "ingest failure due to missing required values has the correct message",
        errorResponse.getMessage(),
        equalTo(String.format("Failed to load data into dataset %s", datasetId)));

    ingestRequest.maxBadRecords(1);

    DataRepoResponse<IngestResponseModel> dataRepoResponseSuccess =
        dataRepoFixtures.ingestJsonDataRaw(steward, datasetId, ingestRequest);

    IngestResponseModel ingestResponseSuccess =
        dataRepoResponseSuccess.getResponseObject().orElseThrow();

    assertThat(
        "allowing 1 bad record means 1 bad record is allowed",
        ingestResponseSuccess.getBadRowCount(),
        equalTo(1L));

    assertThat(
        "allowing a bad row means 2 rows still succeeded",
        ingestResponseSuccess.getRowCount(),
        equalTo(2L));
  }

  @Test
  public void testDatasetCombinedIngest() throws Exception {
    testDatasetCombinedIngest(true);
  }

  @Test
  public void testDatasetCombinedIngestFromApi() throws Exception {
    testDatasetCombinedIngest(false);
  }

  public void testDatasetCombinedIngest(boolean ingestFromFile) throws Exception {
    DatasetSummaryModel summaryModel =
        dataRepoFixtures.createDataset(
            steward, profileId, "dataset-ingest-combined-azure.json", CloudPlatform.AZURE);
    datasetId = summaryModel.getId();
    recordStorageAccount(steward, CollectionType.DATASET, datasetId);

    String controlFileContents;
    try (var resourceStream =
        this.getClass().getResourceAsStream("/dataset-ingest-combined-control-azure.json")) {
      controlFileContents = new String(resourceStream.readAllBytes(), StandardCharsets.UTF_8);
    }

    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table("sample_vcf")
            .profileId(profileId)
            .loadTag(Names.randomizeName("azureCombinedIngestTest"));

    if (ingestFromFile) {
      String controlFile =
          azureBlobIOTestUtility.uploadFileWithContents(
              "dataset-files-ingest-combined.json", controlFileContents);
      ingestRequest.path(controlFile).format(IngestRequestModel.FormatEnum.JSON);
    } else {
      List<Map<String, Object>> data =
          Arrays.stream(controlFileContents.split("\\n"))
              .map(j -> jsonLoader.loadJson(j, new TypeReference<Map<String, Object>>() {}))
              .toList();
      ingestRequest
          .records(Arrays.asList(data.toArray()))
          .format(IngestRequestModel.FormatEnum.ARRAY);
    }

    IngestResponseModel ingestResponse =
        dataRepoFixtures.ingestJsonData(steward, datasetId, ingestRequest);

    dataRepoFixtures.assertCombinedIngestCorrect(ingestResponse, steward);
  }

  public void testMetadataArrayIngest(String arrayIngestTableName, Object records)
      throws Exception {
    IngestRequestModel arrayIngestRequest =
        new IngestRequestModel()
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table(arrayIngestTableName)
            .profileId(profileId)
            .addRecordsItem(records)
            .format(IngestRequestModel.FormatEnum.ARRAY)
            .loadTag(Names.randomizeName("azureArrayIngest"));

    IngestResponseModel arrayIngestResponse =
        dataRepoFixtures.ingestJsonData(steward, datasetId, arrayIngestRequest);
    assertThat("1 row was ingested", arrayIngestResponse.getRowCount(), equalTo(1L));
  }

  private String getSourceStorageAccountPrimarySharedKey() {
    AzureResourceManager client =
        this.azureResourceConfiguration.getClient(
            testConfig.getTargetTenantId(), testConfig.getTargetSubscriptionId());

    return client
        .storageAccounts()
        .getByResourceGroup(
            testConfig.getTargetResourceGroupName(), testConfig.getSourceStorageAccountName())
        .getKeys()
        .iterator()
        .next()
        .value();
  }

  private void deleteCloudResources(String storageAccountName) {
    logger.info("Deleting log analytic workspace {}", storageAccountName);

    LogAnalyticsManager clientLaw =
        azureResourceConfiguration.getLogAnalyticsManagerClient(
            testConfig.getTargetSubscriptionId());
    clientLaw
        .workspaces()
        .delete(testConfig.getTargetManagedResourceGroupName(), storageAccountName);

    logger.info("Deleting storage account {}", storageAccountName);
    AzureResourceManager clientSa =
        azureResourceConfiguration.getClient(testConfig.getTargetSubscriptionId());
    clientSa
        .storageAccounts()
        .deleteByResourceGroup(testConfig.getTargetManagedResourceGroupName(), storageAccountName);
  }

  private void verifyCloudResourceRegions(String storageAccountName, AzureRegion expectedRegion) {
    StorageAccount storageAccount =
        azureResourceConfiguration
            .getClient(testConfig.getTargetSubscriptionId())
            .storageAccounts()
            .getByResourceGroup(testConfig.getTargetManagedResourceGroupName(), storageAccountName);
    assertThat(
        "storage account region is correct",
        storageAccount.region().name(),
        equalTo(expectedRegion.getValue()));

    Workspace logAnalyticsWorkspace =
        azureResourceConfiguration
            .getLogAnalyticsManagerClient(testConfig.getTargetSubscriptionId())
            .workspaces()
            .getByResourceGroup(testConfig.getTargetManagedResourceGroupName(), storageAccountName);
    assertThat(
        "log analytics workspace region is correct",
        logAnalyticsWorkspace.region().name(),
        equalTo(expectedRegion.getValue()));
  }

  private void verifySignedUrl(String signedUrl, User user, String expectedPermissions) {
    BlobUrlParts blobUrlParts = BlobUrlParts.parse(signedUrl);
    assertThat(
        "Signed url contains user",
        blobUrlParts.getCommonSasQueryParameters().getContentDisposition(),
        equalTo(user.getEmail()));
    assertThat(
        "Signed url only contains expected permissions",
        blobUrlParts.getCommonSasQueryParameters().getPermissions(),
        equalTo(expectedPermissions));
    verifyUrlIsAccessible(signedUrl);
  }

  private void verifyUrlIsAccessible(String signedUrl) {
    logger.info("Verifying url %s".formatted(signedUrl));
    try (CloseableHttpClient client = HttpClients.createDefault()) {
      HttpUriRequest request = new HttpHead(signedUrl);
      try (CloseableHttpResponse response = client.execute(request); ) {
        assertThat(
            "URL can be accessed",
            response.getStatusLine().getStatusCode(),
            equalTo(HttpStatus.OK.value()));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String recordStorageAccount(
      TestConfiguration.User user, CollectionType collectionType, UUID collectionId)
      throws Exception {
    String storageAccountName = null;
    switch (collectionType) {
      case DATASET -> {
        DatasetModel dataset =
            dataRepoFixtures.getDataset(
                user, collectionId, List.of(DatasetRequestAccessIncludeModel.ACCESS_INFORMATION));
        storageAccountName = getStorageAccountName(dataset.getAccessInformation().getParquet());
        storageAccounts.add(storageAccountName);
        return storageAccountName;
      }
      case SNAPSHOT -> {
        SnapshotModel snapshot =
            dataRepoFixtures.getSnapshot(
                user, collectionId, List.of(SnapshotRetrieveIncludeModel.ACCESS_INFORMATION));
        storageAccountName = getStorageAccountName(snapshot.getAccessInformation().getParquet());
        storageAccounts.add(getStorageAccountName(snapshot.getAccessInformation().getParquet()));
        return storageAccountName;
      }
    }
    return storageAccountName;
  }

  private String getStorageAccountName(AccessInfoParquetModel accessInfo) {
    return accessInfo.getDatasetId().split("\\.")[0];
  }
}
