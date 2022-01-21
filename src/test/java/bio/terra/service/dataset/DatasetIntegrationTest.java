package bio.terra.service.dataset;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertTrue;

import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.PdaoConstant;
import bio.terra.common.TestUtils;
import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.common.fixtures.DatasetFixtures;
import bio.terra.integration.BigQueryFixtures;
import bio.terra.integration.DataRepoClient;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.DataRepoResponse;
import bio.terra.integration.UsersBase;
import bio.terra.model.AssetModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DataDeletionGcsFileModel;
import bio.terra.model.DataDeletionJsonArrayModel;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetSpecificationModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.EnumerateDatasetModel;
import bio.terra.model.JobModel;
import bio.terra.model.StorageResourceModel;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.iam.IamRole;
import com.google.cloud.WriteChannel;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Charsets;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
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
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

// TODO move me to integration dir
@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@AutoConfigureMockMvc
@Category(Integration.class)
public class DatasetIntegrationTest extends UsersBase {
  private static final String omopDatasetName = "it_dataset_omop";
  private static final String omopDatasetDesc =
      "OMOP schema based on BigQuery schema from https://github.com/OHDSI/CommonDataModel/wiki with extra columns suffixed with _custom";
  private static final String omopDatasetRegion = GoogleRegion.US_CENTRAL1.toString();
  private static Logger logger = LoggerFactory.getLogger(DatasetIntegrationTest.class);

  @Autowired private DataRepoClient dataRepoClient;
  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private AuthService authService;
  @Autowired private TestConfiguration testConfiguration;

  private String stewardToken;
  private UUID datasetId;
  private UUID profileId;

  @Before
  public void setup() throws Exception {
    super.setup();
    stewardToken = authService.getDirectAccessAuthToken(steward().getEmail());
    dataRepoFixtures.resetConfig(steward());
    profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
    datasetId = null;
  }

  @After
  public void teardown() throws Exception {
    dataRepoFixtures.resetConfig(steward());

    if (datasetId != null) {
      dataRepoFixtures.deleteDatasetLog(steward(), datasetId);
    }

    if (profileId != null) {
      dataRepoFixtures.deleteProfileLog(steward(), profileId);
    }
  }

  @Test
  public void datasetHappyPath() throws Exception {
    DatasetSummaryModel summaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "it-dataset-omop.json");
    datasetId = summaryModel.getId();

    logger.info("dataset id is " + summaryModel.getId());
    assertThat(summaryModel.getName(), startsWith(omopDatasetName));
    assertThat(summaryModel.getDescription(), equalTo(omopDatasetDesc));

    DatasetModel datasetModel = dataRepoFixtures.getDataset(steward(), summaryModel.getId());

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
                  dataRepoFixtures.enumerateDatasets(steward());
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

                  GoogleRegion omopDatasetGoogleRegion = GoogleRegion.fromValue(omopDatasetRegion);
                  assert omopDatasetGoogleRegion != null;
                  for (GoogleCloudResource cloudResource : GoogleCloudResource.values()) {
                    StorageResourceModel storage = storageMap.get(cloudResource.toString());
                    GoogleCloudResource resource =
                        GoogleCloudResource.fromValue(storage.getCloudResource());
                    assert resource != null;
                    GoogleRegion expectedRegion;
                    switch (resource) {
                      case BUCKET:
                        expectedRegion = omopDatasetGoogleRegion.getRegionOrFallbackBucketRegion();
                        break;
                      case FIRESTORE:
                        expectedRegion =
                            omopDatasetGoogleRegion.getRegionOrFallbackFirestoreRegion();
                        break;
                      default:
                        expectedRegion = omopDatasetGoogleRegion;
                    }

                    assertThat(
                        String.format("dataset %s region is set", storage.getCloudResource()),
                        storage.getRegion(),
                        equalTo(expectedRegion.toString()));

                    assertThat(
                        "dataset summary has GCP cloud platform",
                        oneDataset.getCloudPlatform(),
                        equalTo(CloudPlatform.GCP));

                    assertThat(
                        "dataset summary has data project",
                        oneDataset.getDataProject(),
                        notNullValue());
                  }

                  found = true;
                  break;
                }
              }
              return found;
            });

    assertTrue("dataset was found in enumeration", metExpectation);

    // test allowable permissions

    dataRepoFixtures.addDatasetPolicyMember(
        steward(), summaryModel.getId(), IamRole.CUSTODIAN, custodian().getEmail());
    DataRepoResponse<EnumerateDatasetModel> enumDatasets =
        dataRepoFixtures.enumerateDatasetsRaw(custodian());
    assertThat(
        "Custodian is authorized to enumerate datasets",
        enumDatasets.getStatusCode(),
        equalTo(HttpStatus.OK));

    assertThat(
        "Default secure monitoring was applied to summary model",
        summaryModel.isSecureMonitoringEnabled(),
        is(false));

    assertThat(
        "Default security classification was propagated to model",
        datasetModel.isSecureMonitoringEnabled(),
        is(false));
  }

  @Test
  public void datasetHappyPathWithPet() throws Exception {
    DatasetSummaryModel summaryModel =
        dataRepoFixtures.createDataset(
            steward(), profileId, "it-dataset-omop.json", CloudPlatform.GCP, true);
    datasetId = summaryModel.getId();

    logger.info("dataset id is " + summaryModel.getId());
    assertThat(summaryModel.getName(), startsWith(omopDatasetName));
    assertThat(summaryModel.getDescription(), equalTo(omopDatasetDesc));

    // We just need to validate the steward is able to read back the dataset (e.g. the pet account
    // resolved correctly)
    DatasetModel datasetModel = dataRepoFixtures.getDataset(steward(), summaryModel.getId());

    assertThat(datasetModel.getName(), startsWith(omopDatasetName));
    assertThat(datasetModel.getDescription(), equalTo(omopDatasetDesc));
  }

  @Test
  public void datasetUnauthorizedPermissionsTest() throws Exception {
    // These should fail because they don't have access to the billing profile
    dataRepoFixtures.createDatasetError(
        custodian(), profileId, "dataset-minimal.json", HttpStatus.UNAUTHORIZED);
    dataRepoFixtures.createDatasetError(
        reader(), profileId, "dataset-minimal.json", HttpStatus.UNAUTHORIZED);

    EnumerateDatasetModel enumDatasetsResp = dataRepoFixtures.enumerateDatasets(reader());
    List<DatasetSummaryModel> items = enumDatasetsResp.getItems();
    if (items != null) {
      for (DatasetSummaryModel datasetModel : items) {
        logger.info(
            String.format(
                "found dataset for reader: %s, created: %s",
                datasetModel.getId(), datasetModel.getCreatedDate()));
      }
    }
    assertThat("Reader does not have access to datasets", enumDatasetsResp.getTotal(), equalTo(0));

    DatasetSummaryModel summaryModel = null;

    summaryModel = dataRepoFixtures.createDataset(steward(), profileId, "dataset-minimal.json");
    datasetId = summaryModel.getId();

    DataRepoResponse<DatasetModel> getDatasetResp =
        dataRepoFixtures.getDatasetRaw(reader(), summaryModel.getId());
    assertThat(
        "Reader is not authorized to get dataset",
        getDatasetResp.getStatusCode(),
        equalTo(HttpStatus.UNAUTHORIZED));

    // make sure reader cannot delete dataset
    DataRepoResponse<JobModel> deleteResp1 =
        dataRepoFixtures.deleteDatasetLaunch(reader(), summaryModel.getId());
    assertThat(
        "Reader is not authorized to delete datasets",
        deleteResp1.getStatusCode(),
        equalTo(HttpStatus.UNAUTHORIZED));

    // right now the authorization for dataset delete is done directly in the controller.
    // so we need to check the response to the delete request for the unauthorized failure
    // once we move the authorization for dataset delete into a separate step,
    // then the check will need two parts, as below:
    // check job launched successfully, check job result is failure with unauthorized
    //            DataRepoResponse<JobModel> jobResp1 = dataRepoFixtures.deleteDatasetLaunch(
    //                reader(), summaryModel.getId());
    //            assertTrue("dataset delete launch succeeded",
    // jobResp1.getStatusCode().is2xxSuccessful());
    //            assertTrue("dataset delete launch response is present",
    // jobResp1.getResponseObject().isPresent());
    //            DataRepoResponse<ErrorModel> deleteResp1 = dataRepoClient.waitForResponse(
    //                reader(), jobResp1, ErrorModel.class);
    //            assertThat("Reader is not authorized to delete datasets",
    //                deleteResp1.getStatusCode(),
    //                equalTo(HttpStatus.UNAUTHORIZED));

    // make sure custodian cannot delete dataset
    DataRepoResponse<JobModel> deleteResp2 =
        dataRepoFixtures.deleteDatasetLaunch(custodian(), summaryModel.getId());
    assertThat(
        "Custodian is not authorized to delete datasets",
        deleteResp2.getStatusCode(),
        equalTo(HttpStatus.UNAUTHORIZED));

    // same comment as above for the reader() delete
    //            DataRepoResponse<JobModel> jobResp2 = dataRepoFixtures.deleteDatasetLaunch(
    //                custodian(), summaryModel.getId());
    //            assertTrue("dataset delete launch succeeded",
    // jobResp2.getStatusCode().is2xxSuccessful());
    //            assertTrue("dataset delete launch response is present",
    // jobResp2.getResponseObject().isPresent());
    //            DataRepoResponse<ErrorModel> deleteResp2 = dataRepoClient.waitForResponse(
    //                custodian(), jobResp2, ErrorModel.class);
    //            assertThat("Custodian is not authorized to delete datasets",
    //                deleteResp2.getStatusCode(),
    //                equalTo(HttpStatus.UNAUTHORIZED));
  }

  @Test
  public void testAssetCreationUndo() throws Exception {
    // create a dataset
    DatasetSummaryModel summaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "it-dataset-omop.json");
    datasetId = summaryModel.getId();
    DatasetModel datasetModel = dataRepoFixtures.getDataset(steward(), summaryModel.getId());
    List<AssetModel> originalAssetList = datasetModel.getSchema().getAssets();

    assertThat(
        "Asset specification is as originally expected", originalAssetList.size(), equalTo(1));
    AssetModel assetModel =
        new AssetModel()
            .name("assetName")
            .rootTable("person")
            .rootColumn("person_id")
            .tables(
                Arrays.asList(
                    DatasetFixtures.buildAssetParticipantTable(),
                    DatasetFixtures.buildAssetSampleTable()))
            .follow(Collections.singletonList("fpk_visit_person"));

    // have the asset creation fail
    // by calling the fault insertion
    dataRepoFixtures.setFault(steward(), ConfigEnum.CREATE_ASSET_FAULT.name(), true);

    // add an asset spec
    dataRepoFixtures.addDatasetAsset(steward(), datasetModel.getId(), assetModel);
    // make sure undo is completed successfully
    DatasetModel datasetModelWAsset = dataRepoFixtures.getDataset(steward(), datasetModel.getId());
    DatasetSpecificationModel datasetSpecificationModel = datasetModelWAsset.getSchema();
    List<AssetModel> assetList = datasetSpecificationModel.getAssets();

    // assert that the asset isn't there
    assertThat("Additional asset specification has never been added", assetList.size(), equalTo(1));
  }

  static DataDeletionTableModel deletionTableFile(String tableName, String path) {
    DataDeletionGcsFileModel deletionGcsFileModel =
        new DataDeletionGcsFileModel()
            .fileType(DataDeletionGcsFileModel.FileTypeEnum.CSV)
            .path(path);
    return new DataDeletionTableModel().tableName(tableName).gcsFileSpec(deletionGcsFileModel);
  }

  static DataDeletionTableModel deletionTableJson(String tableName, List<UUID> rowIds) {
    DataDeletionJsonArrayModel deletionJsonArrayModel =
        new DataDeletionJsonArrayModel().rowIds(rowIds);
    return new DataDeletionTableModel().tableName(tableName).jsonArraySpec(deletionJsonArrayModel);
  }

  static DataDeletionRequest dataDeletionRequest() {
    return new DataDeletionRequest()
        .deleteType(DataDeletionRequest.DeleteTypeEnum.SOFT)
        .specType(DataDeletionRequest.SpecTypeEnum.GCSFILE);
  }

  static List<String> getRowIds(BigQuery bigQuery, DatasetModel dataset, String tableName, Long n)
      throws InterruptedException {

    String tableRef = BigQueryFixtures.makeTableRef(dataset, tableName);
    String sql =
        String.format("SELECT %s FROM %s LIMIT %s", PdaoConstant.PDAO_ROW_ID_COLUMN, tableRef, n);
    TableResult result = BigQueryFixtures.queryWithRetry(sql, bigQuery);

    assertThat("got right num of row ids back", result.getTotalRows(), equalTo(n));
    return StreamSupport.stream(result.getValues().spliterator(), false)
        .map(fieldValues -> fieldValues.get(0).getStringValue())
        .collect(Collectors.toList());
  }

  static String writeListToScratch(String bucket, String prefix, List<String> contents)
      throws IOException {
    Storage storage = StorageOptions.getDefaultInstance().getService();
    String targetPath = "scratch/" + prefix + "/" + UUID.randomUUID() + ".csv";
    BlobInfo blob = BlobInfo.newBuilder(bucket, targetPath).build();
    try (WriteChannel writer = storage.writer(blob)) {
      for (String line : contents) {
        writer.write(ByteBuffer.wrap((line + "\n").getBytes(Charsets.UTF_8)));
      }
    }
    return String.format("gs://%s/%s", blob.getBucket(), targetPath);
  }

  static void assertTableCount(BigQuery bigQuery, DatasetModel dataset, String tableName, Long n)
      throws InterruptedException {

    String sql = "SELECT count(*) FROM " + BigQueryFixtures.makeTableRef(dataset, tableName);
    TableResult result = BigQueryFixtures.queryWithRetry(sql, bigQuery);
    assertThat(
        "count matches", result.getValues().iterator().next().get(0).getLongValue(), equalTo(n));
  }
}
