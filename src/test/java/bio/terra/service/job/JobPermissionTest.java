package bio.terra.service.job;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import bio.terra.common.GcsUtils;
import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration.User;
import bio.terra.integration.DataRepoClient;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.DataRepoResponse;
import bio.terra.integration.IntegrationTestConfiguration;
import bio.terra.integration.Users;
import bio.terra.model.BulkLoadArrayRequestModel;
import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetRequestModelPolicies;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.JobModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = IntegrationTestConfiguration.class)
@ActiveProfiles({"google", "integrationtest"})
@Tag(Integration.TAG)
class JobPermissionTest {
  private static final Logger logger = LoggerFactory.getLogger(JobPermissionTest.class);

  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private GcsUtils gcsUtils;
  @Autowired private DataRepoClient dataRepoClient;
  @Autowired private Users users;

  private Users.TestUsers testUsers;
  private UUID datasetId;
  private UUID profileId;

  private User steward() {
    return testUsers.steward();
  }

  private User admin() {
    return testUsers.admin();
  }

  private User custodian() {
    return testUsers.custodian();
  }

  private User reader() {
    return testUsers.reader();
  }

  @BeforeEach
  public void setup() throws Exception {
    testUsers = users.testUsers();
    dataRepoFixtures.resetConfig(steward());
    profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
    dataRepoFixtures.addPolicyMemberRaw(
        steward(), profileId, IamRole.OWNER, custodian().getEmail(), IamResourceType.SPEND_PROFILE);
  }

  @AfterEach
  public void teardown() throws Exception {
    dataRepoFixtures.resetConfig(steward());

    dataRepoFixtures.deleteDatasetLog(steward(), datasetId);

    dataRepoFixtures.deleteProfileLog(steward(), profileId);
  }

  @Test
  @Disabled("Ignoring until DR-2723 is in so that we can pin job enumeration")
  void testJobPermissions() throws Exception {
    // Create dataset
    DataRepoResponse<JobModel> jobResponse =
        dataRepoFixtures.createDatasetRaw(
            steward(),
            profileId,
            "dataset-ingest-combined-array.json",
            CloudPlatform.GCP,
            false,
            false,
            false,
            false,
            new DatasetRequestModelPolicies().addCustodiansItem(custodian().getEmail()),
            null);
    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.waitForDatasetCreate(steward(), jobResponse);

    datasetId = datasetSummaryModel.getId();

    // Ingest single file
    String ingestBucket = "jade-testdata-useastregion";
    String exomeFilePath =
        gcsUtils.uploadTestFile(
            ingestBucket,
            String.format("jobPermissionTest/%s/fake-exome.g.vcf.gz", datasetId),
            List.of("test vcf file"));

    DataRepoResponse<JobModel> fileIngestJobResponse =
        dataRepoFixtures.ingestFileLaunch(
            steward(),
            datasetId,
            profileId,
            exomeFilePath,
            "/vcfs/downsampled/exome/NA12878_PLUMBING.g.vcf.gz");

    DataRepoResponse<FileModel> fileIngestResponse =
        dataRepoClient.waitForResponse(steward(), fileIngestJobResponse, new TypeReference<>() {});
    assertTrue(fileIngestResponse.getStatusCode().is2xxSuccessful());

    String vcfIndexFilePath =
        gcsUtils.uploadTestFile(
            ingestBucket,
            String.format("jobPermissionTest/%s/fake-vcf-index.g.vcf.gz.tbi", datasetId),
            List.of("test vcf index file"));

    String vcfIndexFilePath2 =
        gcsUtils.uploadTestFile(
            ingestBucket,
            String.format("jobPermissionTest/%s/fake-vcf-index2.g.vcf.gz.tbi", datasetId),
            List.of("another test vcf index file"));

    List<BulkLoadFileModel> vcfIndexLoadModels =
        List.of(
            new BulkLoadFileModel()
                .mimeType("text/plain")
                .description("A downsampled exome gVCF index")
                .sourcePath(vcfIndexFilePath)
                .targetPath("/vcfs/downsampled/exome/NA12878_PLUMBING.g.vcf.gz.tbi"),
            new BulkLoadFileModel()
                .mimeType("text/plain")
                .description("A downsampled wgs gVCF index")
                .sourcePath(vcfIndexFilePath2)
                .targetPath("/vcfs/downsampled/wgs/NA12878_PLUMBING.g.vcf.gz.tbi"));

    // Ingest bulk file array
    DataRepoResponse<JobModel> bulkLoadJobResponse =
        dataRepoFixtures.bulkLoadArrayRaw(
            steward(),
            datasetId,
            new BulkLoadArrayRequestModel()
                .profileId(profileId)
                .loadArray(vcfIndexLoadModels)
                .loadTag("bulk-load-" + datasetId)
                .maxFailedFileLoads(0));
    DataRepoResponse<BulkLoadArrayResultModel> bulkLoadResponse =
        dataRepoClient.waitForResponse(steward(), bulkLoadJobResponse, new TypeReference<>() {});
    assertTrue(bulkLoadResponse.getStatusCode().is2xxSuccessful());

    // Ingest metadata
    IngestRequestModel metadataIngestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.ARRAY)
            .table("sample_vcf")
            .addRecordsItem(Map.of("sample_name", "sample1", "data_type", "vcf"))
            .addRecordsItem(Map.of("sample_name", "sample2", "data_type", "vcf"));

    DataRepoResponse<JobModel> metadataIngestJobResponse =
        dataRepoFixtures.ingestJsonDataLaunch(steward(), datasetId, metadataIngestRequest);
    assertTrue(metadataIngestJobResponse.getStatusCode().is2xxSuccessful());

    // Ingest metadata and files
    IngestRequestModel combinedIngestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table("sample_vcf")
            .path(
                "gs://jade-testdata-useastregion/dataset-ingest-combined-control-duplicates-array.json");

    DataRepoResponse<JobModel> combinedIngestJobResponse =
        dataRepoFixtures.ingestJsonDataLaunch(steward(), datasetId, combinedIngestRequest);
    assertTrue(combinedIngestJobResponse.getStatusCode().is2xxSuccessful());

    // Verify custodian can view jobs
    JobModel datasetCreateJob = jobResponse.getResponseObject().orElseThrow();
    dataRepoFixtures.getJobSuccess(datasetCreateJob.getId(), custodian());

    JobModel fileIngestJob = fileIngestJobResponse.getResponseObject().orElseThrow();
    dataRepoFixtures.getJobSuccess(fileIngestJob.getId(), custodian());

    JobModel bulkLoadJob = bulkLoadJobResponse.getResponseObject().orElseThrow();
    dataRepoFixtures.getJobSuccess(bulkLoadJob.getId(), custodian());

    JobModel metadataIngestJob = metadataIngestJobResponse.getResponseObject().orElseThrow();
    dataRepoFixtures.getJobSuccess(metadataIngestJob.getId(), custodian());

    JobModel combinedIngestJob = combinedIngestJobResponse.getResponseObject().orElseThrow();
    dataRepoFixtures.getJobSuccess(combinedIngestJob.getId(), custodian());

    List<JobModel> jobIds =
        List.of(datasetCreateJob, fileIngestJob, bulkLoadJob, metadataIngestJob, combinedIngestJob);

    assertTrue(
        containsJobIds(dataRepoFixtures.enumerateJobs(admin(), 0, 20), jobIds),
        "Admin can list jobs");
    assertTrue(
        containsJobIds(dataRepoFixtures.enumerateJobs(steward(), 0, 20), jobIds),
        "Steward can list jobs");
    assertTrue(
        containsJobIds(dataRepoFixtures.enumerateJobs(custodian(), 0, 20), jobIds),
        "Custodian can list jobs");
    assertFalse(
        containsJobIds(dataRepoFixtures.enumerateJobs(reader(), 0, 10), jobIds),
        "Reader cannot list jobs");
  }

  private boolean containsJobIds(List<JobModel> jobs, List<JobModel> expectedJobIds) {
    boolean containsJobIds = true;

    Map<String, JobModel> jobsById;
    try {
      jobsById = jobs.stream().collect(Collectors.toMap(JobModel::getId, Function.identity()));
    } catch (IllegalStateException e) {
      logger.error("There appear to be duplicate jobs in the response:\n{}", jobs);
      throw e;
    }
    for (var job : expectedJobIds) {
      if (!jobsById.containsKey(job.getId())) {
        logger.error("Job {} was expected and not found in the jobs response:\n{}", job, jobs);
        containsJobIds = false;
      }
    }
    return containsJobIds;
  }
}
