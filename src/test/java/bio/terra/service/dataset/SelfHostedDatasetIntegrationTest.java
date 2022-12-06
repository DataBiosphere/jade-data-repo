package bio.terra.service.dataset;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import bio.terra.common.GcsUtils;
import bio.terra.common.TestUtils;
import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.integration.BigQueryFixtures;
import bio.terra.integration.DataRepoClient;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.DataRepoResponse;
import bio.terra.integration.SamFixtures;
import bio.terra.integration.TestJobWatcher;
import bio.terra.integration.UsersBase;
import bio.terra.model.BulkLoadArrayRequestModel;
import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadFileResultModel;
import bio.terra.model.DRSAccessMethod;
import bio.terra.model.DRSAccessURL;
import bio.terra.model.DRSObject;
import bio.terra.model.DataDeletionJsonArrayModel;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.JobModel;
import bio.terra.model.SnapshotExportResponseModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.resourcemanagement.google.GoogleResourceManagerService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.storage.StorageRoles;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
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

// TODO move me to integration dir
@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@AutoConfigureMockMvc
@Category(Integration.class)
public class SelfHostedDatasetIntegrationTest extends UsersBase {
  private static Logger logger = LoggerFactory.getLogger(SelfHostedDatasetIntegrationTest.class);

  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private DataRepoClient dataRepoClient;
  @Autowired private AuthService authService;
  @Autowired private GcsUtils gcsUtils;
  @Autowired private SamFixtures samFixtures;
  @Autowired private GoogleResourceManagerService resourceManagerService;

  @Rule @Autowired public TestJobWatcher testWatcher;

  // Disabling check while we debug failing tests.
  // See https://broadworkbench.atlassian.net/browse/DR-2858
  private static final Boolean SHOULD_ASSERT_HTTPS_ACCESSIBILITY = false;

  private String stewardToken;
  private UUID datasetId;
  private UUID snapshotId;
  private String snapshotProject;
  private UUID profileId;
  private List<String> uploadedFiles;
  private String ingestServiceAccount;
  private String ingestBucket;

  @Before
  public void setup() throws Exception {
    super.setup();
    stewardToken = authService.getDirectAccessAuthToken(steward().getEmail());
    dataRepoFixtures.resetConfig(steward());
    profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
    uploadedFiles = new ArrayList<>();
  }

  @After
  public void teardown() throws Exception {
    dataRepoFixtures.resetConfig(steward());

    if (ingestServiceAccount != null && ingestBucket != null) {
      DatasetIntegrationTest.removeServiceAccountRoleFromBucket(
          ingestBucket, ingestServiceAccount, StorageRoles.objectViewer(), snapshotProject);

      DatasetIntegrationTest.removeServiceAccountRoleFromBucket(
          ingestBucket, ingestServiceAccount, StorageRoles.legacyBucketReader(), snapshotProject);
    }

    if (snapshotId != null) {
      dataRepoFixtures.deleteSnapshotLog(steward(), snapshotId);
    }

    if (ingestServiceAccount != null) {
      samFixtures.deleteServiceAccountFromTerra(steward(), ingestServiceAccount);
    }

    if (datasetId != null) {
      dataRepoFixtures.deleteDatasetLog(steward(), datasetId);
    }

    if (profileId != null) {
      dataRepoFixtures.deleteProfileLog(steward(), profileId);
    }

    for (var path : uploadedFiles) {
      gcsUtils.deleteTestFile(path);
    }
  }

  @Test
  public void testSelfHostedDatasetLifecycle() throws Exception {
    testSelfHostedDatasetLifecycle("jade-testdata-useastregion", false);
  }

  @Test
  public void testSelfHostedDatasetWithDedicatedSALifecycle() throws Exception {
    testSelfHostedDatasetLifecycle("jade_testbucket_no_jade_sa", true);
  }

  @Test
  public void testSelfHostedDatasetRequesterPaysLifecycle() throws Exception {
    testSelfHostedDatasetLifecycle("jade_testbucket_requester_pays", true);
  }

  private void testSelfHostedDatasetLifecycle(String ingestBucket, boolean dedicatedServiceAccount)
      throws Exception {

    gcsUtils.fileExists(wgsVcfPath(ingestBucket));

    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createSelfHostedDataset(
            steward(), profileId, "dataset-ingest-combined-array.json", dedicatedServiceAccount);
    datasetId = datasetSummaryModel.getId();

    assertThat(
        "the dataset is marked as self-hosted", datasetSummaryModel.isSelfHosted(), is(true));

    DatasetModel dataset = dataRepoFixtures.getDataset(steward(), datasetId);
    assertThat(
        "the dataset returned from the retrieve endpoint is marked as self-hosted",
        dataset.isSelfHosted(),
        is(true));

    // Authorize the ingest source bucket
    if (dedicatedServiceAccount) {
      ingestServiceAccount = dataset.getIngestServiceAccount();
      this.ingestBucket = ingestBucket;
      // Note: this role gets removed in teardown
      DatasetIntegrationTest.addServiceAccountRoleToBucket(
          ingestBucket,
          ingestServiceAccount,
          StorageRoles.objectViewer(),
          dataset.getDataProject());
      DatasetIntegrationTest.addServiceAccountRoleToBucket(
          ingestBucket,
          ingestServiceAccount,
          StorageRoles.legacyBucketReader(),
          dataset.getDataProject());
    }

    // Ingest a single file
    FileModel exomeVcfModel =
        dataRepoFixtures.ingestFile(
            steward(),
            datasetId,
            profileId,
            exomeVcfPath(ingestBucket),
            "/vcfs/downsampled/exome/NA12878_PLUMBING.g.vcf.gz");

    List<BulkLoadFileModel> vcfIndexLoadModels =
        List.of(
            new BulkLoadFileModel()
                .mimeType("text/plain")
                .description("A downsampled exome gVCF index")
                .sourcePath(
                    String.format(
                        "gs://%s/selfHostedDatasetTest/vcfs/NA12878_PLUMBING_exome.g.vcf.gz.tbi",
                        ingestBucket))
                .targetPath("/vcfs/downsampled/exome/NA12878_PLUMBING.g.vcf.gz.tbi"),
            new BulkLoadFileModel()
                .mimeType("text/plain")
                .description("A downsampled wgs gVCF index")
                .sourcePath(
                    String.format(
                        "gs://%s/selfHostedDatasetTest/vcfs/NA12878_PLUMBING_wgs.g.vcf.gz.tbi",
                        ingestBucket))
                .targetPath("/vcfs/downsampled/wgs/NA12878_PLUMBING.g.vcf.gz.tbi"));

    BulkLoadArrayRequestModel bulkLoadArrayRequestModel =
        new BulkLoadArrayRequestModel()
            .profileId(profileId)
            .loadArray(vcfIndexLoadModels)
            .maxFailedFileLoads(0)
            .loadTag("selfHostedDataset" + datasetId);

    // Bulk ingest files
    BulkLoadArrayResultModel vcfIndicesModel =
        dataRepoFixtures.bulkLoadArray(steward(), datasetId, bulkLoadArrayRequestModel);

    Map<String, String> targetPathToFileId =
        vcfIndicesModel.getLoadFileResults().stream()
            .collect(
                Collectors.toMap(
                    BulkLoadFileResultModel::getTargetPath, BulkLoadFileResultModel::getFileId));
    Map<String, String> fileReplaceMap =
        Map.of(
            "EXOME_VCF_FILE_ID", exomeVcfModel.getFileId(),
            "EXOME_VCF_INDEX_FILE_ID",
                targetPathToFileId.get("/vcfs/downsampled/exome/NA12878_PLUMBING.g.vcf.gz.tbi"),
            "WGS_VCF_INDEX_FILE_ID",
                targetPathToFileId.get("/vcfs/downsampled/wgs/NA12878_PLUMBING.g.vcf.gz.tbi"));

    Stream<String> lines =
        Files.lines(
                Paths.get(ClassLoader.getSystemResource("self-hosted-dataset-ingest.json").toURI()))
            .map(line -> replaceVars(line, fileReplaceMap))
            .map(line -> replaceVars(line, Map.of("INGEST_BUCKET", ingestBucket)));

    // Ingest metadata + 1 combined ingest file
    String ingestPath =
        gcsUtils.uploadTestFile(
            ingestBucket,
            String.format("selfHostedDatasetTest/%s/self-hosted-ingest-control.json", datasetId),
            lines);
    uploadedFiles.add(ingestPath);

    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table("sample_vcf")
            .path(ingestPath);

    dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequest);

    BigQuery bigQuery = BigQueryFixtures.getBigQuery(dataset.getDataProject(), stewardToken);
    DatasetIntegrationTest.assertTableCount(bigQuery, dataset, "sample_vcf", 2L);

    List<Map<String, List<String>>> sampleVcfResults =
        DatasetIntegrationTest.transformStringResults(bigQuery, dataset, "sample_vcf");
    Set<String> ingestedFileIds =
        sampleVcfResults.stream()
            .flatMap(
                result ->
                    Stream.concat(
                        result.get("vcf_file_ref").stream(),
                        result.get("vcf_index_file_ref").stream()))
            .collect(Collectors.toSet());

    assertThat(
        "dataset ingest should result in all fileIds being present in data",
        ingestedFileIds,
        hasSize(4));

    assertThat(
        "all the fileIds in non-combined ingests show up in dataset results",
        ingestedFileIds,
        hasItems(fileReplaceMap.values().toArray(new String[0])));

    SnapshotSummaryModel snapshot =
        dataRepoFixtures.createSnapshot(
            steward(), dataset.getName(), profileId, "dataset-ingest-combined-array-snapshot.json");
    snapshotId = snapshot.getId();
    snapshotProject = snapshot.getDataProject();

    assertThat(
        "a snapshot created from a self-hosted dataset says its self-hosted too",
        snapshot.isSelfHosted(),
        is(true));

    List<DRSObject> collect =
        ingestedFileIds.stream()
            .map(
                fileId -> {
                  try {
                    return dataRepoFixtures.drsGetObject(
                        steward(), String.format("v1_%s_%s", snapshotId, fileId));
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                })
            .toList();

    for (DRSObject drsObject : collect) {
      TestUtils.validateDrsAccessMethods(
          drsObject.getAccessMethods(), stewardToken, SHOULD_ASSERT_HTTPS_ACCESSIBILITY);
      DRSAccessMethod gsAccessMethod =
          drsObject.getAccessMethods().stream()
              .filter(accessMethod -> accessMethod.getType().equals(DRSAccessMethod.TypeEnum.GS))
              .findFirst()
              .orElseThrow();
      assertThat(
          "the drs object's gs-path is in the original ingest bucket",
          GcsUriUtils.parseBlobUri(gsAccessMethod.getAccessUrl().getUrl()).getBucket(),
          equalTo(ingestBucket));
      DRSAccessURL objectAccessUrl =
          dataRepoFixtures
              .getObjectAccessUrl(steward(), drsObject.getId(), gsAccessMethod.getAccessId())
              .getResponseObject()
              .orElseThrow();

      assertThat(
          "TDR was able to create a signed URL",
          objectAccessUrl.getUrl(),
          is(not(emptyOrNullString())));

      // Ensure that the signed URL is accessible
      TestUtils.verifyHttpAccess(objectAccessUrl.getUrl(), Map.of());
    }

    // validate that snapshot export works correctly
    DataRepoResponse<SnapshotExportResponseModel> exportResponse =
        dataRepoFixtures.exportSnapshotLog(steward(), snapshotId, false, false);
    assertThat(
        "self-hosted snapshots can be exported with DRS URIs",
        exportResponse.getResponseObject().isPresent(),
        is(true));

    DataRepoResponse<JobModel> exportSnapshotExpectFailure =
        dataRepoFixtures.exportSnapshot(steward(), snapshotId, true, false);
    DataRepoResponse<ErrorModel> errorResponse =
        dataRepoClient.waitForResponseLog(
            steward(), exportSnapshotExpectFailure, new TypeReference<>() {});

    assertThat(
        "self-hosted snapshots cannot be exported while resolving DRS URIs to gs-paths",
        errorResponse.getErrorObject().orElseThrow().getMessage(),
        containsString("Cannot export GS Paths for self-hosted snapshots"));

    dataRepoFixtures.deleteSnapshotLog(steward(), snapshotId);
    snapshotId = null;

    Map<String, List<String>> rowToDelete =
        sampleVcfResults.stream()
            .filter(result -> result.get("sample_name").get(0).equals("NA12878_wgs"))
            .findFirst()
            .orElseThrow();
    UUID datarepoRowId = UUID.fromString(rowToDelete.get("datarepo_row_id").get(0));

    dataRepoFixtures.deleteData(
        steward(),
        datasetId,
        new DataDeletionRequest()
            .deleteType(DataDeletionRequest.DeleteTypeEnum.SOFT)
            .specType(DataDeletionRequest.SpecTypeEnum.JSONARRAY)
            .tables(
                List.of(
                    new DataDeletionTableModel()
                        .tableName("sample_vcf")
                        .jsonArraySpec(
                            new DataDeletionJsonArrayModel().rowIds(List.of(datarepoRowId))))));

    boolean fileExistsAfterDataDelete = gcsUtils.fileExists(wgsVcfPath(ingestBucket));

    assertThat(
        "files deleted from a self-hosted dataset continue to exist in their source buckets",
        fileExistsAfterDataDelete,
        is(true));

    dataRepoFixtures.deleteDatasetLog(steward(), datasetId);
    datasetId = null;

    boolean fileExistsAfterDatasetDelete = gcsUtils.fileExists(exomeVcfPath(ingestBucket));
    assertThat(
        "files still exist even after self-hosted dataset is deleted",
        fileExistsAfterDatasetDelete,
        is(true));
  }

  private String replaceVars(String inputString, Map<String, String> replacements) {
    String outputString = inputString;
    for (var entry : replacements.entrySet()) {
      outputString = outputString.replaceAll(entry.getKey(), entry.getValue());
    }
    return outputString;
  }

  private String wgsVcfPath(String ingestBucket) {
    return String.format(
        "gs://%s/selfHostedDatasetTest/vcfs/NA12878_PLUMBING_wgs.g.vcf.gz", ingestBucket);
  }

  private String exomeVcfPath(String ingestBucket) {
    return String.format(
        "gs://%s/selfHostedDatasetTest/vcfs/NA12878_PLUMBING_exome.g.vcf.gz", ingestBucket);
  }
}
