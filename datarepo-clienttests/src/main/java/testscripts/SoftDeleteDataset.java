package testscripts;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.api.ResourcesApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.BillingProfileModel;
import bio.terra.datarepo.model.BulkLoadArrayRequestModel;
import bio.terra.datarepo.model.BulkLoadArrayResultModel;
import bio.terra.datarepo.model.BulkLoadFileModel;
import bio.terra.datarepo.model.DataDeletionGcsFileModel;
import bio.terra.datarepo.model.DataDeletionTableModel;
import bio.terra.datarepo.model.DataDeletionRequest;
import bio.terra.datarepo.model.DatasetModel;
import bio.terra.datarepo.model.DatasetSummaryModel;
import bio.terra.datarepo.model.DeleteResponseModel;
import bio.terra.datarepo.model.IngestRequestModel;
import bio.terra.datarepo.model.IngestResponseModel;
import bio.terra.datarepo.model.JobModel;
import com.google.api.client.util.Charsets;
import com.google.cloud.WriteChannel;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.BigQueryUtils;
import utils.DataRepoUtils;
import utils.FileUtils;

public class SoftDeleteDataset extends runner.TestScript {
  private static final Logger logger = LoggerFactory.getLogger(SoftDeleteDataset.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public SoftDeleteDataset() {
    super();
  }

  private URI sourceFileURI;
  private String datasetCreator;
  private BillingProfileModel billingProfileModel;
  private DatasetSummaryModel datasetSummaryModel;

  private Storage storage;
  private String testConfigGetIngestbucket;

  public void setParameters(List<String> parameters) throws Exception {
    if (parameters == null || parameters.size() == 0) {
      throw new IllegalArgumentException(
          "Must provide a URI for the source file in the parameters list");
    } else
      try {
        sourceFileURI = new URI(parameters.get(0));
      } catch (URISyntaxException synEx) {
        throw new RuntimeException("Error parsing source file URI: " + parameters.get(0), synEx);
      }
    logger.debug("Source file URI: {}", sourceFileURI);
  }

  public void setup(Map<String, ApiClient> apiClients) throws Exception {
    // pick the first user to be the dataset creator
    List<String> apiClientList = new ArrayList<>(apiClients.keySet());
    datasetCreator = apiClientList.get(0);

    // get the ApiClient for the dataset creator --- and the snapshot creator?
    ApiClient datasetCreatorClient = apiClients.get(datasetCreator);
    ResourcesApi resourcesApi = new ResourcesApi(datasetCreatorClient);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    storage = StorageOptions.getDefaultInstance().getService();
    testConfigGetIngestbucket = "jade-testdata"; // this could be put in DRUtils

    // create a new profile
    billingProfileModel =
        DataRepoUtils.createProfile(resourcesApi, billingAccount, "profile-simple", true);
    logger.info("Successfully created profile: {}", billingProfileModel.getProfileName());

    // make the create dataset request and wait for the job to finish
    JobModel createDatasetJobResponse =
        DataRepoUtils.createDataset(
            repositoryApi, billingProfileModel.getId(), "dataset-simple.json", true);

    // save a reference to the dataset summary model so we can delete it in cleanup()
    datasetSummaryModel =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, createDatasetJobResponse, DatasetSummaryModel.class);
    logger.info("Successfully created dataset: {}", datasetSummaryModel.getName());

    // load data into the new dataset
    // note that there's a fileref in the dataset
    // ingest a file -- TODO CannedTestData.getMeA1KBFile
    URI sourceUri = new URI("gs://jade-testdata/fileloadprofiletest/1KBfile.txt");

    String targetPath = "/testrunner/IngestFile/" + FileUtils.randomizeName("") + ".txt";

    BulkLoadFileModel fileLoadModel =
        new BulkLoadFileModel()
            .sourcePath(sourceUri.toString())
            .description("IngestFile")
            .mimeType("text/plain")
            .targetPath(targetPath);
    String loadTag = FileUtils.randomizeName("lookupTest");
    BulkLoadArrayRequestModel fileLoadModelArray =
        new BulkLoadArrayRequestModel()
            .profileId(datasetSummaryModel.getDefaultProfileId())
            .loadTag(loadTag)
            .maxFailedFileLoads(0);
    fileLoadModelArray.addLoadArrayItem(fileLoadModel);

    JobModel ingestFileJobResponse =
        repositoryApi.bulkFileLoadArray(datasetSummaryModel.getId(), fileLoadModelArray);
    ingestFileJobResponse = DataRepoUtils.waitForJobToFinish(repositoryApi, ingestFileJobResponse);
    BulkLoadArrayResultModel bulkLoadArrayResultModel =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, ingestFileJobResponse, BulkLoadArrayResultModel.class);
    String fileId = bulkLoadArrayResultModel.getLoadFileResults().get(0).getFileId();

    // ingest the tabular data from the JSON file we just generated
    // generate a JSON file with the fileref
    String jsonLine =
        "{\"VCF_File_Name\":\"name1\", \"Description\":\"description1\", \"VCF_File_Ref\":\""
            + fileId
            + "\"}\n";
    byte[] fileRefBytes = jsonLine.getBytes(StandardCharsets.UTF_8);
    String jsonFileName = FileUtils.randomizeName("this-better-pass") + ".json";
    String dirInCloud = "scratch/testRetrieveSnapshot/";
    String fileRefName = dirInCloud + "/" + jsonFileName;
    String gsPath = FileUtils.createGsPath(fileRefBytes, fileRefName, testConfigGetIngestbucket);

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
  }

  public void userJourney(ApiClient apiClient) throws Exception {
    RepositoryApi repositoryApi = new RepositoryApi(apiClient);

    String datasetId = datasetSummaryModel.getId();
    DatasetModel datasetModel = repositoryApi.retrieveDataset(datasetId);
    String dataProject = datasetModel.getDataProject();
    String tableName = datasetModel.getSchema().getTables().get(0).getName();

    // get row ids for table  TODO combine SQL query w queryForFileRefs in DRSLookup
    String tableRef =
        String.format(
            "`%s.%s.%s`",
            dataProject,
            "datarepo_" + datasetModel.getName(), // TODO const ni BQ utils
            tableName);
    String sqlQuery =
        String.format(
            "SELECT %s FROM %s LIMIT %s",
            "datarepo_row_id", tableRef, 1L); // TODO what is the limit?
    TableResult result = BigQueryUtils.queryBigQuery(dataProject, sqlQuery);
    List<String> rowIds =
        StreamSupport.stream(result.getValues().spliterator(), false)
            .map(fieldValues -> fieldValues.get(0).getStringValue())
            .collect(Collectors.toList());

    logger.debug("Successfully retrieved row ids for table: {}", tableName);

    // write them to GCS
    Storage storage = StorageOptions.getDefaultInstance().getService();
    String targetPath = "scratch/softDel/" + UUID.randomUUID().toString() + ".csv";
    BlobInfo blob = BlobInfo.newBuilder(testConfigGetIngestbucket, targetPath).build();

    try (WriteChannel writer = storage.writer(blob)) {
      for (String line : rowIds) {
        writer.write(ByteBuffer.wrap((line + "\n").getBytes(Charsets.UTF_8)));
      }
    }
    String gcsPath = String.format("gs://%s/%s", blob.getBucket(), targetPath);

    // build the deletion request with pointers to the two files with row ids to soft delete
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
        repositoryApi.applyDatasetDataDeletion(datasetId, dataDeletionRequest);
    softDeleteJobResponse = DataRepoUtils.waitForJobToFinish(repositoryApi, softDeleteJobResponse);
    DeleteResponseModel deleteResponseModel =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, softDeleteJobResponse, DeleteResponseModel.class);
    logger.debug(
        "Successfully soft deleted rows from dataset: {} with state {}",
        datasetSummaryModel.getName(),
        deleteResponseModel.getObjectState());
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

    // delete the profile
    resourcesApi.deleteProfile(billingProfileModel.getId());
    logger.info("Successfully deleted profile: {}", billingProfileModel.getProfileName());
  }
}
