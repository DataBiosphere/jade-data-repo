package testscripts;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.api.ResourcesApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.client.ApiException;
import bio.terra.datarepo.model.BillingProfileModel;
import bio.terra.datarepo.model.BulkLoadArrayRequestModel;
import bio.terra.datarepo.model.BulkLoadArrayResultModel;
import bio.terra.datarepo.model.BulkLoadFileModel;
import bio.terra.datarepo.model.DataDeletionGcsFileModel;
import bio.terra.datarepo.model.DataDeletionRequest;
import bio.terra.datarepo.model.DataDeletionTableModel;
import bio.terra.datarepo.model.DatasetModel;
import bio.terra.datarepo.model.DatasetSummaryModel;
import bio.terra.datarepo.model.DeleteResponseModel;
import bio.terra.datarepo.model.IngestRequestModel;
import bio.terra.datarepo.model.IngestResponseModel;
import bio.terra.datarepo.model.JobModel;
import com.google.api.client.util.Charsets;
import com.google.cloud.bigquery.TableResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.BigQueryUtils;
import utils.DataRepoUtils;
import utils.FileUtils;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.lang.Integer.max;
import static java.lang.Integer.min;
import static java.lang.Math.round;

public class SoftDeleteDatasetManyRows extends runner.TestScript {
  private static final Logger logger = LoggerFactory.getLogger(SoftDeleteDatasetManyRows.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public SoftDeleteDatasetManyRows() {
    super();
  }

  private Integer datasetRowsCount;
  private Integer deleteRowsCount;
  private String datasetCreator;
  private BillingProfileModel billingProfileModel;
  private DatasetSummaryModel datasetSummaryModel;
  private List<DataDeletionRequest> dataDeletionRequestList;

  private String testConfigGetIngestbucket;

    public void setParameters(List<String> parameters) {
        if (parameters == null || parameters.size() == 0) {
            throw new IllegalArgumentException( // or should there be a default number set?
                "Must provide a number of how many rows for the dataset to soft delete from");
        } else {
            // TODO, check for the larger number and make that the one that the is the dataset? Is max / min too lazy?
            datasetRowsCount = max(Integer.parseInt(parameters.get(0)), Integer.parseInt(parameters.get(1)));
            deleteRowsCount = min(Integer.parseInt(parameters.get(0)), Integer.parseInt(parameters.get(1)));
            logger.debug("Source dataset row count: {}, and count to delete: {}", datasetRowsCount, deleteRowsCount);
        }
    }


  public void setup(Map<String, ApiClient> apiClients) throws Exception {
    // pick the first user to be the dataset creator
    List<String> apiClientList = new ArrayList<>(apiClients.keySet());
    datasetCreator = apiClientList.get(0);

    // get the ApiClient for the dataset creator --- and the snapshot creator?
    ApiClient datasetCreatorClient = apiClients.get(datasetCreator);
    ResourcesApi resourcesApi = new ResourcesApi(datasetCreatorClient);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    testConfigGetIngestbucket = "jade-testdata";

    // create a new profile
    billingProfileModel =
        DataRepoUtils.createProfile(resourcesApi, billingAccount, "profile-simple", true);
    logger.info("Successfully created profile: {}", billingProfileModel.getProfileName());

    // make the create dataset request and wait for the job to finish
    JobModel createDatasetJobResponse =
        DataRepoUtils.createDataset(
            repositoryApi, billingProfileModel.getId(), "dataset-simpler.json", true);

    // save a reference to the dataset summary model so we can delete it in cleanup()
    datasetSummaryModel =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, createDatasetJobResponse, DatasetSummaryModel.class);
    logger.info("Successfully created dataset: {}", datasetSummaryModel.getName());

    // load data into the new dataset-- no fileref necessary
    // TODO I need to load in a lot of rows---but no file refs necessary
    ByteArrayOutputStream newRows = new ByteArrayOutputStream();
    for (int i = 0; i < datasetRowsCount; i++) {
        String jsonLine =
            "{\"Name\":\""
            + i
            + "\",  \"Description\":\"description1\"}\n";
        newRows.write((jsonLine).getBytes(Charsets.UTF_8));
    }

    byte[] newRowBytes = newRows.toByteArray();
    String jsonFileName = FileUtils.randomizeName("this-better-pass") + ".json";
    String dirInCloud = "scratch/softDel";
    String fileRefName = dirInCloud + "/" + jsonFileName;
    String gsPath = FileUtils.createGsPath(newRowBytes, fileRefName, testConfigGetIngestbucket);

    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .table("table")
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
    DatasetModel datasetModel = repositoryApi.retrieveDataset(datasetId);
    String dataProject = datasetModel.getDataProject();
    String tableName = datasetModel.getSchema().getTables().get(0).getName();

    // get row ids for table to determine what to delete
      // TODO create multiple soft delete files that can all be used to separately delete rows
      // TODO how do we want to mod out the # we delete?
      // we need many files and those files can be picked randomly during the actual test

    Integer deletionFilesCount = round(datasetRowsCount / deleteRowsCount);
    Long limit = deleteRowsCount.longValue();

      for (int j = 0; j < deletionFilesCount; j++) { // TODO this will get the same limit / group each query, yeah?
          String sqlQuery =
              BigQueryUtils.constructQuery(
                  dataProject,
                  BigQueryUtils.getDatasetName(datasetModel.getName()),
                  tableName,
                  "datarepo_row_id",
                  limit);

          TableResult result = BigQueryUtils.queryBigQuery(dataProject, sqlQuery);
          List<String> rowIds =
              StreamSupport.stream(result.getValues().spliterator(), false)
                  .map(fieldValues -> fieldValues.get(0).getStringValue())
                  .collect(Collectors.toList());

          logger.info("Successfully retrieved row ids during round {} for table: {}", j, tableName);
          // write to GCS
          String csvFileName = FileUtils.randomizeName("this-too-better-pass")+ j + ".csv";
          String csvFileRefName = dirInCloud + "/" + csvFileName;

          ByteArrayOutputStream output = new ByteArrayOutputStream();
          for (String line : rowIds) {
              output.write((line + "\n").getBytes(Charsets.UTF_8));
          }
          byte[] bytes = output.toByteArray();
          String gcsPath = FileUtils.createGsPath(bytes, csvFileRefName, testConfigGetIngestbucket);

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

          dataDeletionRequestList.add(dataDeletionRequest);
      }
  }

  public void userJourney(ApiClient apiClient) throws Exception {
    RepositoryApi repositoryApi = new RepositoryApi(apiClient);

    // send off the soft delete requests
    dataDeletionRequestList.forEach( dataDeletionRequest -> {
        JobModel softDeleteJobResponse;
        DeleteResponseModel deleteResponseModel;
        try {
        softDeleteJobResponse = repositoryApi.applyDatasetDataDeletion(datasetSummaryModel.getId(), dataDeletionRequest);
        softDeleteJobResponse = DataRepoUtils.waitForJobToFinish(repositoryApi, softDeleteJobResponse);
        deleteResponseModel =
            DataRepoUtils.expectJobSuccess(
                repositoryApi, softDeleteJobResponse, DeleteResponseModel.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.debug(
            "Successfully soft deleted rows from dataset: {} with state {}",
            datasetSummaryModel.getName(),
            deleteResponseModel.getObjectState()); // TODO is this line needed?
        }
    );



  }

  public void cleanup(Map<String, ApiClient> apiClients) throws Exception {
    // get the ApiClient for the dataset creator
    ApiClient datasetCreatorClient = apiClients.get(datasetCreator);
    ResourcesApi resourcesApi = new ResourcesApi(datasetCreatorClient);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    // make the delete request and wait for the job to finish
    JobModel deleteDatasetJobResponse = repositoryApi.deleteDataset(datasetSummaryModel.getId());
    deleteDatasetJobResponse =
        DataRepoUtils.waitForJobToFinish(repositoryApi, deleteDatasetJobResponse);
    DataRepoUtils.expectJobSuccess(
        repositoryApi, deleteDatasetJobResponse, DeleteResponseModel.class);
    logger.info("Successfully deleted dataset: {}", datasetSummaryModel.getName());

    // delete scratch files
    FileUtils.cleanupScratchFiles(testConfigGetIngestbucket);

    // delete the profile
    resourcesApi.deleteProfile(billingProfileModel.getId());
    logger.info("Successfully deleted profile: {}", billingProfileModel.getProfileName());
  }
}
