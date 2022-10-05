package bio.terra.service.job;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import bio.terra.common.GcsUtils;
import bio.terra.common.category.Integration;
import bio.terra.integration.DataRepoClient;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.DataRepoResponse;
import bio.terra.integration.UsersBase;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@AutoConfigureMockMvc
@Category(Integration.class)
public class JobPermissionTest extends UsersBase {
  private static final Logger logger = LoggerFactory.getLogger(JobPermissionTest.class);

  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private GcsUtils gcsUtils;
  @Autowired private DataRepoClient dataRepoClient;

  private UUID datasetId;
  private UUID profileId;

  @Before
  public void setup() throws Exception {
    super.setup();
    dataRepoFixtures.resetConfig(steward());
    profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
    dataRepoFixtures.addPolicyMemberRaw(
        steward(), profileId, IamRole.OWNER, custodian().getEmail(), IamResourceType.SPEND_PROFILE);
  }

  @After
  public void teardown() throws Exception {
    dataRepoFixtures.resetConfig(steward());

    dataRepoFixtures.deleteDatasetLog(steward(), datasetId);

    dataRepoFixtures.deleteProfileLog(steward(), profileId);
  }

  @Test
  public void testJobPermissions() throws Exception {
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
            new DatasetRequestModelPolicies().addCustodiansItem(custodian().getEmail()));
    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.waitForDatasetCreate(steward(), jobResponse);

    datasetId = datasetSummaryModel.getId();

    // Ingest single file
    String ingestBucket = "jade-testdata-useastregion";
    String exomeFilePath =
        gcsUtils.uploadTestFile(
            ingestBucket,
            String.format("jobPermissionTest/%s/fake-exome.g.vcf.gz", datasetId),
            Stream.of("test vcf file"));

    DataRepoResponse<JobModel> fileIngestJobResponse =
        dataRepoFixtures.ingestFileLaunch(
            steward(),
            datasetId,
            profileId,
            exomeFilePath,
            "/vcfs/downsampled/exome/NA12878_PLUMBING.g.vcf.gz");

    DataRepoResponse<FileModel> fileIngestResponse =
        dataRepoClient.waitForResponse(steward(), fileIngestJobResponse, new TypeReference<>() {});
    assert (fileIngestResponse.getStatusCode().is2xxSuccessful());

    String vcfIndexFilePath =
        gcsUtils.uploadTestFile(
            ingestBucket,
            String.format("jobPermissionTest/%s/fake-vcf-index.g.vcf.gz.tbi", datasetId),
            Stream.of("test vcf index file"));

    String vcfIndexFilePath2 =
        gcsUtils.uploadTestFile(
            ingestBucket,
            String.format("jobPermissionTest/%s/fake-vcf-index2.g.vcf.gz.tbi", datasetId),
            Stream.of("another test vcf index file"));

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
    assert (bulkLoadResponse.getStatusCode().is2xxSuccessful());

    // Ingest metadata
    IngestRequestModel metadataIngestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.ARRAY)
            .table("sample_vcf")
            .addRecordsItem(Map.of("sample_name", "sample1", "data_type", "vcf"))
            .addRecordsItem(Map.of("sample_name", "sample2", "data_type", "vcf"));

    DataRepoResponse<JobModel> metadataIngestJobResponse =
        dataRepoFixtures.ingestJsonDataLaunch(steward(), datasetId, metadataIngestRequest);
    assert (metadataIngestJobResponse.getStatusCode().is2xxSuccessful());

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
    assert (combinedIngestJobResponse.getStatusCode().is2xxSuccessful());

    // Verify custodian can view jobs
    JobModel datasetCreateJob = jobResponse.getResponseObject().get();
    dataRepoFixtures.getJobSuccess(datasetCreateJob.getId(), custodian());

    JobModel fileIngestJob = fileIngestJobResponse.getResponseObject().get();
    dataRepoFixtures.getJobSuccess(fileIngestJob.getId(), custodian());

    JobModel bulkLoadJob = bulkLoadJobResponse.getResponseObject().get();
    dataRepoFixtures.getJobSuccess(bulkLoadJob.getId(), custodian());

    JobModel metadataIngestJob = metadataIngestJobResponse.getResponseObject().get();
    dataRepoFixtures.getJobSuccess(metadataIngestJob.getId(), custodian());

    JobModel combinedIngestJob = combinedIngestJobResponse.getResponseObject().get();
    dataRepoFixtures.getJobSuccess(combinedIngestJob.getId(), custodian());

    List<JobModel> jobIds =
        List.of(datasetCreateJob, fileIngestJob, bulkLoadJob, metadataIngestJob, combinedIngestJob);

    assertTrue(
        "Admin can list jobs", containsJobIds(dataRepoFixtures.enumerateJobs(admin()), jobIds));
    assertTrue(
        "Steward can list jobs", containsJobIds(dataRepoFixtures.enumerateJobs(steward()), jobIds));
    assertTrue(
        "Custodian can list jobs",
        containsJobIds(dataRepoFixtures.enumerateJobs(custodian()), jobIds));
    assertFalse(
        "Reader cannot list jobs",
        containsJobIds(dataRepoFixtures.enumerateJobs(reader()), jobIds));
  }

  private boolean containsJobIds(List<JobModel> jobs, List<JobModel> expectedJobIds) {
    boolean containsJobIds = true;

    Map<String, JobModel> jobsById;
    try {
      jobsById = jobs.stream().collect(Collectors.toMap(JobModel::getId, j -> j));
    } catch (IllegalStateException e) {
      logger.error("There appear to be duplicate jobs in the response:\n{}", jobs);
      throw e;
    }
    for (var job : expectedJobIds) {
      if (!jobsById.containsKey(job.getId())) {
        logger.error("Job {} was expected and not found", job);
        containsJobIds = false;
      }
    }
    return containsJobIds;
  }
}
