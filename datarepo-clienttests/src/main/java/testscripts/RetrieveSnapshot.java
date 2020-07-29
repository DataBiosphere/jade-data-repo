package testscripts;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.api.ResourcesApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.BillingProfileModel;
import bio.terra.datarepo.model.BulkLoadArrayRequestModel;
import bio.terra.datarepo.model.BulkLoadArrayResultModel;
import bio.terra.datarepo.model.BulkLoadFileModel;
import bio.terra.datarepo.model.DatasetSummaryModel;
import bio.terra.datarepo.model.DeleteResponseModel;
import bio.terra.datarepo.model.IngestRequestModel;
import bio.terra.datarepo.model.IngestResponseModel;
import bio.terra.datarepo.model.JobModel;
import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.datarepo.model.SnapshotSummaryModel;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.DataRepoUtils;
import utils.FileUtils;

public class RetrieveSnapshot extends runner.TestScript {
  private static final Logger logger = LoggerFactory.getLogger(RetrieveSnapshot.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public RetrieveSnapshot() {
    super();
  }

  private String datasetCreator;
  private BillingProfileModel billingProfileModel;
  private DatasetSummaryModel datasetSummaryModel;
  private SnapshotSummaryModel snapshotSummaryModel;

  private Storage storage;
  private String testConfigGetIngestbucket;

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

    // make the create snapshot request and wait for the job to finish
    JobModel createSnapshotJobResponse =
        DataRepoUtils.createSnapshot(
            repositoryApi, datasetSummaryModel, "snapshot-simple.json", true);

    // save a reference to the snapshot summary model so we can delete it in cleanup()
    snapshotSummaryModel =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, createSnapshotJobResponse, SnapshotSummaryModel.class);
    logger.info("Successfully created snapshot: {}", snapshotSummaryModel.getName());
  }

  public void userJourney(ApiClient apiClient) throws Exception {
    RepositoryApi repositoryApi = new RepositoryApi(apiClient);

    SnapshotModel snapshotModel = repositoryApi.retrieveSnapshot(snapshotSummaryModel.getId());
    logger.debug(
        "Successfully retrieved snaphot: {}, data project: {}",
        snapshotModel.getName(),
        snapshotModel.getDataProject());
  }

  public void cleanup(Map<String, ApiClient> apiClients) throws Exception {
    // get the ApiClient for the dataset creator
    ApiClient datasetCreatorClient = apiClients.get(datasetCreator);
    ResourcesApi resourcesApi = new ResourcesApi(datasetCreatorClient);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    // make the delete request and wait for the job to finish
    JobModel deleteSnapshotJobResponse = repositoryApi.deleteSnapshot(snapshotSummaryModel.getId());
    deleteSnapshotJobResponse =
        DataRepoUtils.waitForJobToFinish(repositoryApi, deleteSnapshotJobResponse);
    DataRepoUtils.expectJobSuccess(
        repositoryApi, deleteSnapshotJobResponse, DeleteResponseModel.class);
    logger.info("Successfully deleted snapshot: {}", snapshotSummaryModel.getName());

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
