package scripts.testscripts;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.BillingProfileModel;
import bio.terra.datarepo.model.BulkLoadArrayRequestModel;
import bio.terra.datarepo.model.BulkLoadArrayResultModel;
import bio.terra.datarepo.model.BulkLoadFileModel;
import bio.terra.datarepo.model.DatasetModel;
import bio.terra.datarepo.model.DatasetSummaryModel;
import bio.terra.datarepo.model.DeleteResponseModel;
import bio.terra.datarepo.model.IngestRequestModel;
import bio.terra.datarepo.model.IngestResponseModel;
import bio.terra.datarepo.model.JobModel;
import bio.terra.datarepo.model.SnapshotSummaryModel;
import com.google.cloud.storage.BlobId;
import common.utils.FileUtils;
import common.utils.IngestServiceAccountUtils;
import common.utils.StorageUtils;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestUserSpecification;
import scripts.testscripts.baseclasses.BillingProfileUsers;
import scripts.utils.DataRepoUtils;
import scripts.utils.tdrwrapper.DataRepoWrap;
import scripts.utils.tdrwrapper.exception.DataRepoBadRequestClientException;
import scripts.utils.tdrwrapper.exception.DataRepoNotFoundClientException;

public class BillingProfileInUseTest extends BillingProfileUsers {
  private static final Logger logger = LoggerFactory.getLogger(BillingProfileInUseTest.class);
  private static List<BlobId> scratchFiles = new ArrayList<>();
  private String stewardsEmail;

  /** Public constructor so that this class can be instantiated via reflection. */
  public BillingProfileInUseTest() {
    super();
  }

  public void setup(List<TestUserSpecification> testUsers) throws Exception {
    super.setup(testUsers);
  }

  public void setParameters(List<String> parameters) throws Exception {
    if (parameters == null || parameters.size() == 0) {
      throw new IllegalArgumentException(
          "Must provide a number of files to load in the parameters list");
    } else {
      stewardsEmail = parameters.get(0);
    }
  }

  public void userJourney(TestUserSpecification testUser) throws Exception {
    DataRepoWrap ownerUser1Api = DataRepoWrap.wrapFactory(ownerUser1, server);
    DataRepoWrap ownerUser2Api = DataRepoWrap.wrapFactory(ownerUser2, server);
    DataRepoWrap userUserApi = DataRepoWrap.wrapFactory(userUser, server);

    BillingProfileModel profile = null;
    DatasetSummaryModel dataset = null;
    SnapshotSummaryModel snapshot = null;

    try {
      // owner1 creates the profile and grants ownerUser2 and userUser the "user" role
      profile = ownerUser1Api.createProfile(billingAccount, "profile_permission_test", true);
      UUID profileId = profile.getId();
      logger.info("ProfileId: {}", profileId);

      ownerUser1Api.addProfilePolicyMember(profileId, "user", ownerUser2.userEmail);
      ownerUser1Api.addProfilePolicyMember(profileId, "user", userUser.userEmail);
      dumpPolicies(ownerUser1Api, profileId);

      // owner2 creates a dataset and grants userUser the "custodian" role
      dataset = ownerUser2Api.createDataset(profileId, "dataset-simple.json", true);
      ownerUser2Api.addDatasetPolicyMember(dataset.getId(), "custodian", userUser.userEmail);
      String ingestBucket = "jade-testdata";
      DatasetModel datasetModel = ownerUser2Api.retrieveDataset(dataset.getId());
      IngestServiceAccountUtils.grantIngestBucketPermissionsToDedicatedSa(
          datasetModel, ingestBucket, server);
      ingestDataIntoDataset(dataset, profileId, ingestBucket);

      // user creates a snapshot
      snapshot =
          userUserApi.createSnapshot(profileId, "snapshot-simple.json", dataset.getName(), true);

      // attempt to delete profile should fail due to dataset and snapshot dependency
      tryDeleteProfile(ownerUser1Api, profileId, false);

      assertThat(
          userUserApi.deleteSnapshot(snapshot.getId()).getObjectState(),
          equalTo(DeleteResponseModel.ObjectStateEnum.DELETED));
      snapshot = null;

      // attempt to delete profile should fail due to dataset dependency
      tryDeleteProfile(ownerUser1Api, profileId, false);

      IngestServiceAccountUtils.revokeIngestBucketPermissionsFromDedicatedSa(
          ownerUser2, datasetModel, ingestBucket, server);
      assertThat(
          ownerUser2Api.deleteDataset(dataset.getId()).getObjectState(),
          equalTo(DeleteResponseModel.ObjectStateEnum.DELETED));
      dataset = null;

      // attempt to delete profile should succeed
      tryDeleteProfile(ownerUser1Api, profileId, true);
      profile = null;
    } catch (Exception e) {
      logger.error("Error in journey", e);
      e.printStackTrace();
      throw e;
    } finally {
      if (snapshot != null) {
        userUserApi.deleteSnapshot(snapshot.getId());
      }
      if (dataset != null) {
        ownerUser2Api.deleteDataset(dataset.getId());
      }
      if (profile != null) {
        ownerUser1Api.deleteProfile(profile.getId());
      }
    }
  }

  private void tryDeleteProfile(DataRepoWrap wrap, UUID id, boolean expectSuccess)
      throws Exception {
    boolean success;
    try {
      wrap.deleteProfile(id);
      success = true;
    } catch (DataRepoBadRequestClientException | DataRepoNotFoundClientException ex) {
      success = false;
    }
    assertThat("success meets expectations", success, equalTo(expectSuccess));
  }

  private void ingestDataIntoDataset(
      DatasetSummaryModel dataset, UUID profileId, String ingestBucket) throws Exception {
    // load data into the new dataset
    // note that there's a fileref in the dataset
    // ingest a file -- TODO CannedTestData.getMeA1KBFile
    // get the ApiClient for the snapshot creator, same as the dataset creator
    ApiClient datasetCreatorClient = DataRepoUtils.getClientForTestUser(userUser, server);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    String sourcePath = "gs://" + ingestBucket + "/fileloadprofiletest/1KBfile.txt";
    URI sourceUri = new URI(sourcePath);
    String targetPath = "/testrunner/IngestFile/" + FileUtils.randomizeName("") + ".txt";

    BulkLoadFileModel fileLoadModel =
        new BulkLoadFileModel()
            .sourcePath(sourceUri.toString())
            .description("IngestFile")
            .mimeType("text/plain")
            .targetPath(targetPath);
    String loadTag = FileUtils.randomizeName("lookupTest");
    BulkLoadArrayRequestModel fileLoadModelArray =
        new BulkLoadArrayRequestModel().profileId(profileId).loadTag(loadTag).maxFailedFileLoads(0);
    fileLoadModelArray.addLoadArrayItem(fileLoadModel);

    JobModel ingestFileJobResponse =
        repositoryApi.bulkFileLoadArray(dataset.getId(), fileLoadModelArray);
    ingestFileJobResponse =
        DataRepoUtils.waitForJobToFinish(repositoryApi, ingestFileJobResponse, userUser);
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
    // load a JSON file that contains the table rows to load into the test bucket
    String jsonFileName = FileUtils.randomizeName("this-better-pass") + ".json";
    String fileRefName = "scratch/testDRSLookup/" + jsonFileName;

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
        repositoryApi.ingestDataset(dataset.getId(), ingestRequest);

    ingestTabularDataJobResponse =
        DataRepoUtils.waitForJobToFinish(repositoryApi, ingestTabularDataJobResponse, userUser);
    IngestResponseModel ingestResponse =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, ingestTabularDataJobResponse, IngestResponseModel.class);
    logger.info("Successfully loaded data into dataset: {}", ingestResponse.getDataset());
  }
}
