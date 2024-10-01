package bio.terra.service.dataset;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.DataRepoResponse;
import bio.terra.integration.SamFixtures;
import bio.terra.integration.UsersBase;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.service.resourcemanagement.google.GoogleResourceManagerService;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
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
public class DatasetControlFilesIntegrationTest extends UsersBase {

  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private JsonLoader jsonLoader;
  @Autowired private TestConfiguration testConfiguration;

  private UUID datasetId;
  private UUID profileId;
  private String ingestBucket;

  @Before
  public void setup() throws Exception {
    super.setup();
    dataRepoFixtures.resetConfig(steward());
    profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
  }

  @After
  public void teardown() throws Exception {
    dataRepoFixtures.resetConfig(steward());

    if (datasetId != null) {
      dataRepoFixtures.deleteDataset(steward(), datasetId, ingestBucket);
    }

    if (profileId != null) {
      dataRepoFixtures.deleteProfileLog(steward(), profileId);
    }
  }

  @Test
  public void testCombinedMetadataDataIngest() throws Exception {
    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "dataset-ingest-combined-array.json");
    datasetId = datasetSummaryModel.getId();

    // Initial uses bulk mode
    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table("sample_vcf")
            .bulkMode(true)
            .path(
                "gs://jade-testdata-useastregion/dataset-ingest-combined-control-duplicates-array.json");

    IngestResponseModel ingestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);

    dataRepoFixtures.assertCombinedIngestCorrect(ingestResponse, steward());

    assertThat(
        "All 4 rows were ingested, including the one with duplicate files",
        ingestResponse.getRowCount(),
        equalTo(4L));

    IngestRequestModel secondIngestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table("sample_vcf")
            .resolveExistingFiles(true)
            .path(
                "gs://jade-testdata-useastregion/dataset-ingest-combined-control-duplicates-array-2.json");

    IngestResponseModel secondIngestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, secondIngestRequest);

    assertThat(
        "A row with a different load tag but same files ingested correctly",
        secondIngestResponse.getRowCount(),
        equalTo(2L));

    assertThat(
        "Only 2 files were loaded because the other 2 already exist",
        secondIngestResponse.getLoadResult().getLoadSummary().getTotalFiles(),
        equalTo(2));

    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
    dataRepoFixtures.assertDatasetTableCount(steward(), dataset, "sample_vcf", 6);

    IngestRequestModel thirdIngestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .ignoreUnknownValues(false)
            .maxBadRecords(1)
            .maxFailedFileLoads(2)
            .table("sample_vcf")
            .path(
                "gs://jade-testdata-useastregion/dataset-ingest-combined-control-duplicates-array-3.json");

    DataRepoResponse<IngestResponseModel> thirdIngestResponse =
        dataRepoFixtures.ingestJsonDataRaw(steward(), datasetId, thirdIngestRequest);

    IngestResponseModel thirdResponseModel = thirdIngestResponse.getResponseObject().orElseThrow();

    assertThat(
        "Row ingest fails when duplicate files present and not told to resolve",
        thirdResponseModel.getBadRowCount(),
        equalTo(1L));

    assertThat(
        "Files fail to ingest if already exist and not told to resolve",
        thirdResponseModel.getLoadResult().getLoadSummary().getFailedFiles(),
        equalTo(2));
  }

  @Test
  public void testMaxBadRecords() throws Exception {
    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "dataset-ingest-combined-array.json");
    datasetId = datasetSummaryModel.getId();

    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .maxFailedFileLoads(0)
            .table("sample_vcf")
            .path(
                "gs://jade-testdata-useastregion/dataset-ingest-combined-control-duplicates-array.json");

    dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);

    IngestRequestModel secondIngestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table("sample_vcf")
            .resolveExistingFiles(false)
            .path(
                "gs://jade-testdata-useastregion/dataset-ingest-combined-control-duplicates-array.json");

    DataRepoResponse<IngestResponseModel> secondIngestResponse =
        dataRepoFixtures.ingestJsonDataRaw(steward(), datasetId, secondIngestRequest);

    assertThat(
        "The ingest fails if there were more bad rows than badRowCount",
        secondIngestResponse.getErrorObject().isPresent(),
        is(true));

    ErrorModel errorModel = secondIngestResponse.getErrorObject().get();

    assertThat(
        "The failed file loads have error messages", errorModel.getErrorDetail(), hasSize(4));
  }

  @Test
  public void testSourcePathAuth() throws Exception {
    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "dataset-ingest-combined-array.json");
    datasetId = datasetSummaryModel.getId();

    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .maxFailedFileLoads(0)
            .table("sample_vcf")
            .path(
                "gs://jade-testdata-useastregion/dataset-ingest-combined-control-unauth-source-path.json");

    DataRepoResponse<IngestResponseModel> ingestResponse =
        dataRepoFixtures.ingestJsonDataRaw(steward(), datasetId, ingestRequest);

    assertThat(
        "The ingest fails if there are any source paths that the user doesn't have access to",
        ingestResponse.getErrorObject().isPresent(),
        is(true));

    ErrorModel errorModel = ingestResponse.getErrorObject().get();

    assertThat(
        "The number of error messages should be 5 (the max) + 1 to note the error messages have been truncated.",
        errorModel.getErrorDetail(),
        hasSize(6));
  }

  @Test
  public void testDirectIngestSourcePathAuth() throws Exception {
    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "dataset-ingest-combined-array.json");
    datasetId = datasetSummaryModel.getId();
    Map<String, Object> data =
        jsonLoader.loadObject("test-direct-ingest-auth.json", new TypeReference<>() {});
    IngestRequestModel request = dataRepoFixtures.buildSimpleIngest("sample_vcf", List.of(data));
    DataRepoResponse<IngestResponseModel> ingestResponse =
        dataRepoFixtures.ingestJsonDataRaw(steward(), datasetId, request);

    assertThat(
        "The ingest fails if there are any errors accessing the source path(s)",
        ingestResponse.getErrorObject().isPresent(),
        is(true));

    ErrorModel errorModel = ingestResponse.getErrorObject().get();

    assertThat(
        "The source bucket does not exist.",
        errorModel.getErrorDetail().get(0),
        containsString("The specified bucket does not exist"));
  }

  @Test
  public void testCopyingOfControlFiles() throws Exception {
    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "dataset-ingest-combined-array.json");
    datasetId = datasetSummaryModel.getId();

    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table("sample_vcf")
            .path("gs://jade-testdata/dataset-ingest-combined-control-duplicates-array.json");

    IngestResponseModel ingestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);

    dataRepoFixtures.assertCombinedIngestCorrect(ingestResponse, steward());

    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
    List<String> rowIds = dataRepoFixtures.getRowIds(steward(), dataset, "sample_vcf", 1);
    String rowIdsPath =
        DatasetIntegrationTest.writeListToScratch(
            testConfiguration.getIngestbucket(), "softDel", rowIds);

    List<DataDeletionTableModel> dataDeletionTableModels =
        List.of(DatasetIntegrationTest.deletionTableFile("sample_vcf", rowIdsPath));
    DataDeletionRequest request =
        DatasetIntegrationTest.dataDeletionRequest().tables(dataDeletionTableModels);

    dataRepoFixtures.deleteData(steward(), datasetId, request);

    // We should see that one row was deleted.
    dataRepoFixtures.assertDatasetTableCount(steward(), dataset, "sample_vcf", 3);
  }

  @Test
  public void testCopyingOfControlFilesMultiRegion() throws Exception {
    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDataset(
            steward(), profileId, "dataset-ingest-combined-array-us.json");
    datasetId = datasetSummaryModel.getId();

    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table("sample_vcf")
            .path("gs://jade-testdata/dataset-ingest-combined-control-duplicates-array.json");

    IngestResponseModel ingestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);

    dataRepoFixtures.assertCombinedIngestCorrect(ingestResponse, steward());

    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
    List<String> rowIds = dataRepoFixtures.getRowIds(steward(), dataset, "sample_vcf", 1);
    String rowIdsPath =
        DatasetIntegrationTest.writeListToScratch(
            testConfiguration.getIngestbucket(), "softDel", rowIds);

    List<DataDeletionTableModel> dataDeletionTableModels =
        List.of(DatasetIntegrationTest.deletionTableFile("sample_vcf", rowIdsPath));
    DataDeletionRequest request =
        DatasetIntegrationTest.dataDeletionRequest().tables(dataDeletionTableModels);

    dataRepoFixtures.deleteData(steward(), datasetId, request);

    // We should see that one row was deleted.
    dataRepoFixtures.assertDatasetTableCount(steward(), dataset, "sample_vcf", 3);
  }

  @Test
  public void testInvalidControlFile() throws Exception {
    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDataset(
            steward(), profileId, "dataset-ingest-combined-array-us.json");
    datasetId = datasetSummaryModel.getId();

    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table("sample_vcf")
            .path("gs://jade-testdata/dataset-combined-ingest-control-file-invalid.json");

    ErrorModel error = dataRepoFixtures.ingestJsonDataFailure(steward(), datasetId, ingestRequest);
    assertThat(
        "Malformed source path field throws error",
        error.getErrorDetail().get(0),
        containsString(
            "Unrecognized field \"source_path\" (class bio.terra.model.BulkLoadFileModel), not marked as ignorable"));
    assertThat(
        "Error message detail should include that the targetPath was not defined in the control file.",
        error.getErrorDetail().get(1),
        containsString("Error: The following required field(s) were not defined: targetPath"));
  }

  @Test
  public void interactionsFromRequesterPaysBucket() throws Exception {
    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "dataset-ingest-combined-array.json");
    datasetId = datasetSummaryModel.getId();

    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table("sample_vcf")
            .path(
                "gs://jade_testbucket_requester_pays/dataset-ingest-combined-control-duplicates-array.json");

    IngestResponseModel ingestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);

    dataRepoFixtures.assertCombinedIngestCorrect(ingestResponse, steward());

    assertThat(
        "All 4 rows were ingested, including the one with duplicate files",
        ingestResponse.getRowCount(),
        equalTo(4L));

    // Soft delete from file
    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
    List<String> rowIds = dataRepoFixtures.getRowIds(steward(), dataset, "sample_vcf", 4);
    String rowIdsPath =
        DatasetIntegrationTest.writeListToScratch(
            "jade_testbucket_requester_pays",
            "softDel",
            rowIds.subList(0, 2),
            dataset.getDataProject());

    // build the deletion request with pointers to the two files with row ids to soft delete
    List<DataDeletionTableModel> dataDeletionTableModels =
        List.of(DatasetIntegrationTest.deletionTableFile("sample_vcf", rowIdsPath));
    DataDeletionRequest request =
        DatasetIntegrationTest.dataDeletionRequest().tables(dataDeletionTableModels);

    // send off the soft delete request
    dataRepoFixtures.deleteData(steward(), datasetId, request);

    // We should only see 2 records now
    dataRepoFixtures.getRowIds(steward(), dataset, "sample_vcf", 2);
  }

  @Test
  public void interactionsWithPerDatasetServiceAccount() throws Exception {
    ingestBucket = "jade_testbucket_no_jade_sa";
    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDatasetWithOwnServiceAccount(
            steward(), profileId, "dataset-ingest-combined-array.json");

    datasetId = datasetSummaryModel.getId();
    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);

    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table("sample_vcf")
            .path(
                String.format(
                    "gs://%s/dataset-ingest-combined-control-duplicates-array.json", ingestBucket));

    DataRepoResponse<IngestResponseModel> ingestResponseBeforeGrant =
        dataRepoFixtures.ingestJsonDataRaw(steward(), datasetId, ingestRequest);

    assertThat(
        "ingest failed before granting SA to source bucket",
        ingestResponseBeforeGrant.getStatusCode(),
        equalTo(HttpStatus.UNAUTHORIZED));

    assertThat(
        "error message is the right one",
        ingestResponseBeforeGrant.getErrorObject().map(ErrorModel::getMessage).orElse(""),
        containsString(String.format("TDR cannot access bucket %s.", ingestBucket)));

    // Now grant the reader role on the source bucket to the ingest service account
    assertThat(
        "the ingest service account is not the global one",
        dataset.getIngestServiceAccount(),
        startsWith("tdr-ingest-sa"));
    dataRepoFixtures.grantIngestBucketPermissionsToDedicatedSa(dataset, ingestBucket);

    IngestResponseModel ingestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);

    dataRepoFixtures.assertCombinedIngestCorrect(ingestResponse, steward());

    assertThat(
        "All 4 rows were ingested, including the one with duplicate files",
        ingestResponse.getRowCount(),
        equalTo(4L));
  }
}
