package testscripts;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.DataDeletionGcsFileModel;
import bio.terra.datarepo.model.DataDeletionRequest;
import bio.terra.datarepo.model.DataDeletionTableModel;
import bio.terra.datarepo.model.DatasetModel;
import bio.terra.datarepo.model.DeleteResponseModel;
import bio.terra.datarepo.model.IngestRequestModel;
import bio.terra.datarepo.model.IngestResponseModel;
import bio.terra.datarepo.model.JobModel;
import com.google.api.client.util.Charsets;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableResult;
import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestUserSpecification;
import testscripts.baseclasses.SimpleDataset;
import utils.BigQueryUtils;
import utils.DataRepoUtils;
import utils.FileUtils;

public class SoftDeleteDatasetManyRows extends SimpleDataset {
  private static final Logger logger = LoggerFactory.getLogger(SoftDeleteDatasetManyRows.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public SoftDeleteDatasetManyRows() {
    super();
  }

  private Integer datasetRowsCount;
  private Integer deleteRowsCount;
  private String dataProject;
  private String tableName;
  private DatasetModel datasetModel;

  private String testConfigGetIngestbucket;
  String dirInCloud = "scratch/softDel";

  public void setParameters(List<String> parameters) {
    if (parameters == null || parameters.size() == 0) {
      throw new IllegalArgumentException( // or should there be a default number set?
          "Must provide the number of rows in each soft delete request");
    } else {
      // Hard-code the size of the dataset (say 10k rows or 1 million if we can ingest that in a
      // reasonable amount of time).
      // Accept as a parameter to this test script the number of rows in each soft delete request.
      datasetRowsCount = 10000;
      deleteRowsCount = Integer.parseInt(parameters.get(0));
      // Validate that the number of rows in each soft delete request <= hard-coded dataset size.
      if (deleteRowsCount > datasetRowsCount) {
        throw new IllegalArgumentException(
            "Number of rows to soft delete per request must be less than number of rows in the dataset");
      }
      logger.debug(
          "Source dataset row count: {}, and count to delete per request: {}",
          datasetRowsCount,
          deleteRowsCount);
    }
  }

  public void setup(List<TestUserSpecification> testUsers) throws Exception {
    // create the profile and dataset
    super.setup(testUsers);

    // get the ApiClient for the dataset creator
    ApiClient datasetCreatorClient = DataRepoUtils.getClientForTestUser(datasetCreator, server);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    testConfigGetIngestbucket = "jade-testdata"; // this could be put in DRUtils

    // load data into the new dataset-- no fileref necessary
    // load 10,000,000 rows into the dataset table -- 10 mil rows at once is successful---but we
    // time out w 100,000,000. To speed up this test, use bulk_test_delete_10000.json

    String fileRefName = "scratch/bulk_test_delete/bulk_test_delete_10000000.json";
    String gsPath = String.format("gs://%s/%s", testConfigGetIngestbucket, fileRefName);
    logger.info("Loading data from: {}", gsPath);

    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .table("vcf_file")
            .path(gsPath);
    JobModel ingestTabularDataJobResponse =
        repositoryApi.ingestDataset(datasetSummaryModel.getId(), ingestRequest);

    ingestTabularDataJobResponse =
        DataRepoUtils.waitForJobToFinish(repositoryApi, ingestTabularDataJobResponse);
    IngestResponseModel ingestResponse =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, ingestTabularDataJobResponse, IngestResponseModel.class);
    logger.info("Successfully loaded data into dataset: {}", ingestResponse.getDataset());

    String datasetId = datasetSummaryModel.getId();
    datasetModel = repositoryApi.retrieveDataset(datasetId);
    dataProject = datasetModel.getDataProject();
    tableName = datasetModel.getSchema().getTables().get(0).getName();
  }

  public void userJourney(TestUserSpecification testUser) throws Exception {
    ApiClient apiClient = DataRepoUtils.getClientForTestUser(testUser, server);
    RepositoryApi repositoryApi = new RepositoryApi(apiClient);
    // create dataDeletionRequest: query BQ, write row ids into file, load into GCS, build request

    // For the BQ query to get the rowids to delete, pick a random chunk of rows
    // So if the parameter is 5 (rows per soft delete request), then we select 5 rows from BQ at a
    // random offset.
    long leftLimit = 0L;
    long rightLimit = datasetRowsCount;
    long offset =
        leftLimit
            + (long)
                (Math.random()
                    * (rightLimit - leftLimit)); // (starting point for soft delete request)
    long limit = deleteRowsCount.longValue(); // (rows per soft delete request)

    String sqlQuery =
        BigQueryUtils.buildSelectQuery(
            dataProject,
            BigQueryUtils.getDatasetName(datasetModel.getName()),
            tableName,
            "datarepo_row_id",
            limit,
            offset);

    BigQuery bigQueryClient =
        BigQueryUtils.getClientForTestUser(datasetCreator, datasetModel.getDataProject());
    TableResult result = BigQueryUtils.queryBigQuery(bigQueryClient, sqlQuery);
    List<String> rowIds =
        StreamSupport.stream(result.getValues().spliterator(), false)
            .map(fieldValues -> fieldValues.get(0).getStringValue())
            .collect(Collectors.toList());

    logger.info("Successfully retrieved {} row ids for table: {}", rowIds.size(), tableName);
    // write to GCS
    String csvFileName = FileUtils.randomizeName("this-too-better-pass") + ".csv";
    String csvFileRefName = dirInCloud + "/" + csvFileName;

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    for (String line : rowIds) {
      output.write((line + "\n").getBytes(Charsets.UTF_8));
    }
    byte[] bytes = output.toByteArray();
    String gcsPath = FileUtils.createGsPath(bytes, csvFileRefName, testConfigGetIngestbucket);
    logger.info("Successfully created file for row ids to soft delete: {}", gcsPath);

    // build the deletion request with pointers to the file with row ids to soft delete
    DataDeletionGcsFileModel deletionGcsFileModel =
        new DataDeletionGcsFileModel()
            .fileType(DataDeletionGcsFileModel.FileTypeEnum.CSV)
            .path(gcsPath);
    DataDeletionTableModel deletionTableFile =
        new DataDeletionTableModel().tableName(tableName).gcsFileSpec(deletionGcsFileModel);
    List<DataDeletionTableModel> dataDeletionTableModels =
        Collections.singletonList(deletionTableFile);
    DataDeletionRequest dataDeletionRequest =
        new DataDeletionRequest()
            .deleteType(DataDeletionRequest.DeleteTypeEnum.SOFT)
            .specType(DataDeletionRequest.SpecTypeEnum.GCSFILE)
            .tables(dataDeletionTableModels);

    // send off the soft delete request
    JobModel softDeleteJobResponse =
        repositoryApi.applyDatasetDataDeletion(datasetSummaryModel.getId(), dataDeletionRequest);
    softDeleteJobResponse = DataRepoUtils.waitForJobToFinish(repositoryApi, softDeleteJobResponse);
    DeleteResponseModel deleteResponseModel =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, softDeleteJobResponse, DeleteResponseModel.class);

    logger.debug(
        "Successfully soft deleted rows from dataset: {} with state {}",
        datasetSummaryModel.getName(),
        deleteResponseModel.getObjectState());
  }

  public void cleanup(List<TestUserSpecification> testUsers) throws Exception {
    // delete the profile and dataset
    super.cleanup(testUsers);

    // delete scratch files
    FileUtils.cleanupScratchFiles(testConfigGetIngestbucket);
  }
}
