package bio.terra.service.dataset;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.integration.BigQueryFixtures;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.DataRepoResponse;
import bio.terra.integration.UsersBase;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import com.google.cloud.bigquery.BigQuery;
import java.util.List;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
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
public class DatasetControlFilesIntegrationTest extends UsersBase {

  @Autowired private AuthService authService;
  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private TestConfiguration testConfiguration;

  private String stewardToken;
  private UUID profileId;

  @Before
  public void setup() throws Exception {
    super.setup();
    stewardToken = authService.getDirectAccessAuthToken(steward().getEmail());
    dataRepoFixtures.resetConfig(steward());
    profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
  }

  @After
  public void teardown() throws Exception {
    dataRepoFixtures.resetConfig(steward());

    if (profileId != null) {
      dataRepoFixtures.deleteProfileLog(steward(), profileId);
    }
  }

  @Test
  public void testCombinedMetadataDataIngest() throws Exception {
    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "dataset-ingest-combined-array.json");
    UUID datasetId = datasetSummaryModel.getId();

    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table("sample_vcf")
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
    BigQuery bigQuery = BigQueryFixtures.getBigQuery(dataset.getDataProject(), stewardToken);

    DatasetIntegrationTest.assertTableCount(bigQuery, dataset, "sample_vcf", 6L);

    IngestRequestModel thirdIngestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
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
    UUID datasetId = datasetSummaryModel.getId();

    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
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
  public void testCopyingOfControlFiles() throws Exception {
    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "dataset-ingest-combined-array.json");
    UUID datasetId = datasetSummaryModel.getId();

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
    BigQuery bigQuery = BigQueryFixtures.getBigQuery(dataset.getDataProject(), stewardToken);

    List<String> rowIds = DatasetIntegrationTest.getRowIds(bigQuery, dataset, "sample_vcf", 1L);
    String rowIdsPath =
        DatasetIntegrationTest.writeListToScratch(
            testConfiguration.getIngestbucket(), "softDel", rowIds);

    List<DataDeletionTableModel> dataDeletionTableModels =
        List.of(DatasetIntegrationTest.deletionTableFile("sample_vcf", rowIdsPath));
    DataDeletionRequest request =
        DatasetIntegrationTest.dataDeletionRequest().tables(dataDeletionTableModels);

    dataRepoFixtures.deleteData(steward(), datasetId, request);

    // We should see that one row was deleted.
    DatasetIntegrationTest.assertTableCount(bigQuery, dataset, "sample_vcf", 3L);
  }

  @Test
  public void testCopyingOfControlFilesMultiRegion() throws Exception {
    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDataset(
            steward(), profileId, "dataset-ingest-combined-array-us.json");
    UUID datasetId = datasetSummaryModel.getId();

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
    BigQuery bigQuery = BigQueryFixtures.getBigQuery(dataset.getDataProject(), stewardToken);

    List<String> rowIds = DatasetIntegrationTest.getRowIds(bigQuery, dataset, "sample_vcf", 1L);
    String rowIdsPath =
        DatasetIntegrationTest.writeListToScratch(
            testConfiguration.getIngestbucket(), "softDel", rowIds);

    List<DataDeletionTableModel> dataDeletionTableModels =
        List.of(DatasetIntegrationTest.deletionTableFile("sample_vcf", rowIdsPath));
    DataDeletionRequest request =
        DatasetIntegrationTest.dataDeletionRequest().tables(dataDeletionTableModels);

    dataRepoFixtures.deleteData(steward(), datasetId, request);

    // We should see that one row was deleted.
    DatasetIntegrationTest.assertTableCount(bigQuery, dataset, "sample_vcf", 3L);
  }
}
