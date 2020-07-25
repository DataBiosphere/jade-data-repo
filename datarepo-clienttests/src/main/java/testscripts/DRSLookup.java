package testscripts;

import bio.terra.datarepo.api.DataRepositoryServiceApi;
import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.api.ResourcesApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.BillingProfileModel;
import bio.terra.datarepo.model.BulkLoadArrayRequestModel;
import bio.terra.datarepo.model.BulkLoadArrayResultModel;
import bio.terra.datarepo.model.BulkLoadFileModel;
import bio.terra.datarepo.model.DRSObject;
import bio.terra.datarepo.model.DatasetSummaryModel;
import bio.terra.datarepo.model.DeleteResponseModel;
import bio.terra.datarepo.model.IngestRequestModel;
import bio.terra.datarepo.model.IngestResponseModel;
import bio.terra.datarepo.model.JobModel;
import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.datarepo.model.SnapshotSummaryModel;
import bio.terra.datarepo.model.TableModel;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.Storage;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import utils.BigQueryUtils;
import utils.DataRepoUtils;
import utils.FileUtils;

public class DRSLookup extends runner.TestScript {

  /** Public constructor so that this class can be instantiated via reflection. */
  public DRSLookup() {
    super();
  }

  private String datasetCreator;
  private BillingProfileModel billingProfileModel;
  private DatasetSummaryModel datasetSummaryModel;
  private SnapshotModel snapshotModel;

  private Storage storage;
  private String testConfigGetIngestbucket;
  private String dirObjectId;

  public void setup(Map<String, ApiClient> apiClients) throws Exception {
    // pick the first user to be the dataset creator
    List<String> apiClientList = new ArrayList<>(apiClients.keySet());
    datasetCreator = apiClientList.get(0);

    // get the ApiClient for the dataset creator --- and the snapshot creator?
    ApiClient datasetCreatorClient = apiClients.get(datasetCreator);
    ResourcesApi resourcesApi = new ResourcesApi(datasetCreatorClient);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    testConfigGetIngestbucket = "jade-testdata"; // this could be put in DRUtils

    // create a new profile
    billingProfileModel =
        DataRepoUtils.createProfile(resourcesApi, billingAccount, "profile-simple", true);

    System.out.println("successfully created profile: " + billingProfileModel.getProfileName());

    // make the create dataset request and wait for the job to finish
    JobModel createDatasetJobResponse =
        DataRepoUtils.createDataset(
            repositoryApi, billingProfileModel.getId(), "dataset-simple.json", true);

    // save a reference to the dataset summary model so we can delete it in cleanup()
    datasetSummaryModel =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, createDatasetJobResponse, DatasetSummaryModel.class);

    System.out.println("successfully created dataset: " + datasetSummaryModel.getName());

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
    List<BulkLoadFileModel> bulkLoadFileModelList = new ArrayList<>();
    bulkLoadFileModelList.add(fileLoadModel);
    BulkLoadArrayRequestModel fileLoadModelArray =
        new BulkLoadArrayRequestModel()
            .profileId(datasetSummaryModel.getDefaultProfileId())
            .loadArray(bulkLoadFileModelList);
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
    System.out.println("successfully loaded data into dataset: " + ingestResponse.getDataset());

    // make the create snapshot request and wait for the job to finish
    JobModel createSnapshotJobResponse =
        DataRepoUtils.createSnapshot(
            repositoryApi, datasetSummaryModel, "snapshot-simple.json", true);

    // save a reference to the snapshot summary model so we can delete it in cleanup()
    SnapshotSummaryModel snapshotSummaryModel =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, createSnapshotJobResponse, SnapshotSummaryModel.class);
    System.out.println("successfully created snapshot: " + snapshotSummaryModel.getName());

    // now go and retrieve the file Id that should be stored in the snapshot
    snapshotModel = repositoryApi.retrieveSnapshot(snapshotSummaryModel.getId());
    TableModel tableModel = snapshotModel.getTables().get(0);
    String queryForFileRefs =
        "SELECT * FROM "
            + snapshotModel.getDataProject()
            + "."
            + snapshotModel.getName()
            + "."
            + tableModel.getName();

    TableResult result =
        BigQueryUtils.queryBigQuery(snapshotModel.getDataProject(), queryForFileRefs);

    ArrayList<String> fileRefs = new ArrayList<>();
    result.iterateAll().forEach(r -> fileRefs.add(r.get("VCF_File_Ref").getStringValue()));
    // fileRefs should only be 1 in size
    String fileModelFileId = fileRefs.get(0);
    String freshFileId = fileModelFileId.split("_")[2];
    dirObjectId = "v1_" + snapshotSummaryModel.getId() + "_" + freshFileId;
  }

  public void userJourney(ApiClient apiClient) throws Exception {
    DataRepositoryServiceApi dataRepositoryServiceApi = new DataRepositoryServiceApi(apiClient);
    DRSObject object = dataRepositoryServiceApi.getObject(dirObjectId, false);

    System.out.println(
        "successfully retrieved drs object: "
            + object.getName()
            + " with id: "
            + dirObjectId
            + ", data project: "
            + snapshotModel.getDataProject());
  }

  public void cleanup(Map<String, ApiClient> apiClients) throws Exception {
    // get the ApiClient for the dataset creator
    ApiClient datasetCreatorClient = apiClients.get(datasetCreator);
    ResourcesApi resourcesApi = new ResourcesApi(datasetCreatorClient);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    // make the delete request and wait for the job to finish
    JobModel deleteSnapshotJobResponse = repositoryApi.deleteSnapshot(snapshotModel.getId());
    deleteSnapshotJobResponse =
        DataRepoUtils.waitForJobToFinish(repositoryApi, deleteSnapshotJobResponse);
    DataRepoUtils.expectJobSuccess(
        repositoryApi, deleteSnapshotJobResponse, DeleteResponseModel.class);
    System.out.println("successfully deleted snapshot: " + snapshotModel.getName());

    // make the delete request and wait for the job to finish
    JobModel deleteDatasetJobResponse = repositoryApi.deleteDataset(datasetSummaryModel.getId());
    deleteDatasetJobResponse =
        DataRepoUtils.waitForJobToFinish(repositoryApi, deleteDatasetJobResponse);
    DataRepoUtils.expectJobSuccess(
        repositoryApi, deleteDatasetJobResponse, DeleteResponseModel.class);
    System.out.println("successfully deleted dataset: " + datasetSummaryModel.getName());

    // delete scratch files
    FileUtils.cleanupScratchFiles(testConfigGetIngestbucket);

    // delete the profile
    resourcesApi.deleteProfile(billingProfileModel.getId());
    System.out.println("successfully deleted profile: " + billingProfileModel.getProfileName());
  }
}
