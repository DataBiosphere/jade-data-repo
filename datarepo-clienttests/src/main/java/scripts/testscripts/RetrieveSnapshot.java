package scripts.testscripts;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.api.ResourcesApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.client.ApiException;
import bio.terra.datarepo.model.BillingProfileModel;
import bio.terra.datarepo.model.BulkLoadArrayRequestModel;
import bio.terra.datarepo.model.BulkLoadArrayResultModel;
import bio.terra.datarepo.model.BulkLoadFileModel;
import bio.terra.datarepo.model.DeleteResponseModel;
import bio.terra.datarepo.model.IngestRequestModel;
import bio.terra.datarepo.model.IngestResponseModel;
import bio.terra.datarepo.model.JobModel;
import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.datarepo.model.SnapshotSummaryModel;
import com.google.cloud.storage.BlobId;
import common.utils.FileUtils;
import common.utils.StorageUtils;
import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestUserSpecification;
import scripts.testscripts.baseclasses.SimpleDataset;
import scripts.utils.DataRepoUtils;
import scripts.utils.SAMUtils;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class RetrieveSnapshot extends SimpleDataset {
  private static final Logger logger = LoggerFactory.getLogger(RetrieveSnapshot.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public RetrieveSnapshot() {
    super();
  }

  private SnapshotSummaryModel snapshotSummaryModel;
  private List<BlobId> scratchFiles = new ArrayList<>();
  private boolean deleteProfile;

  public void setup(List<TestUserSpecification> testUsers) throws Exception {
    // Specify the name of a profile to use for the test with the TEST_RUNNER_BILLING_PROFILE_NAME
    // env variable.
    // If it's not specified or the profile isn't found, a new one with a randomly generated name
    // will be created
    String profileNameRaw = System.getenv("TEST_RUNNER_BILLING_PROFILE_NAME");
    Optional<String> profileName;
    if (StringUtils.isBlank(profileNameRaw)) {
      profileName = Optional.empty();
      deleteProfile = true;
    } else {
      profileName = Optional.ofNullable(profileNameRaw);
      deleteProfile = false;
    }

    // pick the a user that is a Data Repo steward to be the dataset creator
    datasetCreator = SAMUtils.findTestUserThatIsDataRepoSteward(testUsers, server);

    // get the ApiClient for the snapshot creator, same as the dataset creator
    ApiClient datasetCreatorClient = DataRepoUtils.getClientForTestUser(datasetCreator, server);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);
    ResourcesApi resourcesApi = new ResourcesApi(datasetCreatorClient);
    BillingProfileModel billingProfileModel =
        profileName
            .map(
                p -> {
                  try {
                    List<BillingProfileModel> profiles =
                        resourcesApi.enumerateProfiles(0, 100).getItems();

                    BillingProfileModel foundProfile =
                        IterableUtils.find(
                            profiles, m -> StringUtils.equalsIgnoreCase(p, m.getProfileName()));
                    if (foundProfile == null) {
                      logger.info("Could not find a profile named {}", p);
                    }
                    return foundProfile;
                  } catch (final ApiException e) {
                    throw new RuntimeException("Error fetching profile", e);
                  }
                })
            .orElse(null);

    super.setup(testUsers, billingProfileModel);

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

    String scratchFileBucketName = "jade-testdata";
    BlobId scratchFileTabularData =
        StorageUtils.writeBytesToFile(
            StorageUtils.getClientForServiceAccount(server.testRunnerServiceAccount),
            scratchFileBucketName,
            fileRefName,
            fileRefBytes);
    scratchFiles.add(scratchFileTabularData); // make sure the scratch file gets cleaned up later
    String gsPath = StorageUtils.blobIdToGSPath(scratchFileTabularData);

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

  public void userJourney(TestUserSpecification testUser) throws Exception {
    ApiClient apiClient = DataRepoUtils.getClientForTestUser(testUser, server);
    RepositoryApi repositoryApi = new RepositoryApi(apiClient);

    SnapshotModel snapshotModel = repositoryApi.retrieveSnapshot(snapshotSummaryModel.getId());
    logger.debug(
        "Successfully retrieved snaphot: {}, data project: {}",
        snapshotModel.getName(),
        snapshotModel.getDataProject());
  }

  public void cleanup(List<TestUserSpecification> testUsers) throws Exception {
    // get the ApiClient for the dataset creator
    ApiClient datasetCreatorClient = DataRepoUtils.getClientForTestUser(datasetCreator, server);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    // make the delete request and wait for the job to finish
    JobModel deleteSnapshotJobResponse = repositoryApi.deleteSnapshot(snapshotSummaryModel.getId());
    deleteSnapshotJobResponse =
        DataRepoUtils.waitForJobToFinish(repositoryApi, deleteSnapshotJobResponse);
    DataRepoUtils.expectJobSuccess(
        repositoryApi, deleteSnapshotJobResponse, DeleteResponseModel.class);
    logger.info("Successfully deleted snapshot: {}", snapshotSummaryModel.getName());

    // delete the profile and dataset
    super.cleanup(testUsers, deleteProfile);

    // delete the scratch files used for ingesting tabular data
    StorageUtils.deleteFiles(
        StorageUtils.getClientForServiceAccount(server.testRunnerServiceAccount), scratchFiles);
  }
}
