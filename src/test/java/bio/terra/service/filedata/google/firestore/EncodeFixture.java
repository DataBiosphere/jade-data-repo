package bio.terra.service.filedata.google.firestore;

import bio.terra.common.TestUtils;
import bio.terra.common.auth.AuthService;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.integration.BigQueryFixtures;
import bio.terra.integration.DataRepoClient;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.model.BulkLoadArrayRequestModel;
import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadFileResultModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRetrieveIncludeModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.filedata.google.gcs.GcsChannelWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import java.io.BufferedReader;
import java.net.URI;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "integrationtest"})
@Component
public class EncodeFixture {
  private static final Logger logger = LoggerFactory.getLogger(EncodeFixture.class);

  @Autowired private JsonLoader jsonLoader;
  @Autowired private ObjectMapper objectMapper;
  @Autowired private DataRepoClient dataRepoClient;
  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private AuthService authService;
  @Autowired private TestConfiguration testConfiguration;

  public static class SetupResult {
    private final UUID profileId;
    private final UUID datasetId;
    private final SnapshotSummaryModel summaryModel;

    public SetupResult(UUID profileId, UUID datasetId, SnapshotSummaryModel summaryModel) {
      this.profileId = profileId;
      this.datasetId = datasetId;
      this.summaryModel = summaryModel;
    }

    public UUID getDatasetId() {
      return datasetId;
    }

    public UUID getProfileId() {
      return profileId;
    }

    public SnapshotSummaryModel getSummaryModel() {
      return summaryModel;
    }
  }

  // Create dataset, load files and tables. Create and return snapshot.
  // Steward owns dataset; custodian is custodian on dataset; reader has access to the snapshot.

  /**
   * Create dataset, load files and tables. Create and return snapshot. Steward owns dataset;
   * custodian is custodian on dataset; reader has access to the snapshot.
   *
   * @param shouldAssertBqDatasetAccessible if set, verify that the snapshot's underlying BigQuery
   *     dataset is accessible to the reader
   */
  public SetupResult setupEncode(
      TestConfiguration.User steward,
      TestConfiguration.User custodian,
      TestConfiguration.User reader,
      boolean shouldAssertBqDatasetAccessible)
      throws Exception {

    UUID profileId = dataRepoFixtures.createBillingProfile(steward).getId();
    dataRepoFixtures.addPolicyMember(
        steward, profileId, IamRole.USER, custodian.getEmail(), IamResourceType.SPEND_PROFILE);

    DatasetSummaryModel datasetSummary =
        dataRepoFixtures.createDataset(steward, profileId, "encodefiletest-dataset.json");
    UUID datasetId = datasetSummary.getId();

    dataRepoFixtures.addDatasetPolicyMember(
        steward, datasetId, IamRole.CUSTODIAN, custodian.getEmail());

    // Parse the input data and load the files; generate revised data file
    String stewardToken = authService.getDirectAccessAuthToken(steward.getEmail());
    Storage stewardStorage = dataRepoFixtures.getStorage(stewardToken);
    String targetPath = loadFiles(datasetSummary.getId(), profileId, steward, stewardStorage);

    // Load the tables
    IngestRequestModel request = dataRepoFixtures.buildSimpleIngest("file", targetPath);
    dataRepoFixtures.ingestJsonData(steward, datasetId, request);

    // Delete the targetPath file
    deleteLoadFile(steward, targetPath);

    request = dataRepoFixtures.buildSimpleIngest("donor", "encodetest/donor.json");
    dataRepoFixtures.ingestJsonData(steward, datasetId, request);

    // Delete the scratch blob
    Blob scratchBlob =
        stewardStorage.get(BlobId.of(testConfiguration.getIngestbucket(), targetPath));
    if (scratchBlob != null) {
      scratchBlob.delete();
    }

    // At this point, we have files and tabular data. Let's make a data snapshot!
    SnapshotSummaryModel snapshotSummary =
        dataRepoFixtures.createSnapshot(
            custodian, datasetSummary.getName(), profileId, "encodefiletest-snapshot.json");

    dataRepoFixtures.addSnapshotPolicyMember(
        custodian, snapshotSummary.getId(), IamRole.STEWARD, steward.getEmail());

    // TODO: Fix use of IamProviderInterface - see DR-494
    dataRepoFixtures.addSnapshotPolicyMember(
        custodian, snapshotSummary.getId(), IamRole.READER, reader.getEmail());

    if (shouldAssertBqDatasetAccessible) {
      // We wait here for SAM to sync. We expect this to take 5 minutes. It can take more as recent
      // issues have shown. We make a BigQuery request as the test to see that READER has access.
      // We need to get the snapshot, rather than the snapshot summary in order to make a query.
      // TODO: Add dataProject to SnapshotSummaryModel?
      SnapshotModel snapshotModel =
          dataRepoFixtures.getSnapshot(
              custodian,
              snapshotSummary.getId(),
              List.of(SnapshotRetrieveIncludeModel.ACCESS_INFORMATION));
      logger.info(
          "Checking BQ access for snapshot {} in data project {} with BQ dataset named {}",
          snapshotModel.getName(),
          snapshotModel.getAccessInformation().getBigQuery().getProjectId(),
          snapshotModel.getAccessInformation().getBigQuery().getDatasetName());

      String readerToken = authService.getDirectAccessAuthToken(reader.getEmail());
      BigQuery bigQueryReader =
          BigQueryFixtures.getBigQuery(snapshotModel.getDataProject(), readerToken);

      BigQueryFixtures.assertBqDatasetAccessible(
          bigQueryReader,
          snapshotModel.getAccessInformation().getBigQuery().getProjectId(),
          snapshotModel.getAccessInformation().getBigQuery().getDatasetName());

      logger.info("Successfully checked access");
    } else {
      logger.info("Skipping BQ dataset access check for snapshot {}", snapshotSummary.getName());
    }

    return new SetupResult(profileId, datasetId, snapshotSummary);
  }

  private String loadFiles(
      UUID datasetId, UUID profileId, TestConfiguration.User user, Storage storage)
      throws Exception {
    // Open the source data from the bucket
    // Open target data in bucket
    // Read one line at a time - unpack into pojo
    // Ingest the files, substituting the file ids
    // Generate JSON and write the line to scratch
    String rndSuffix = UUID.randomUUID().toString() + ".json";
    String loadData = "scratch/lf_loaddata" + rndSuffix;

    // For a bigger test use encodetest/file.json (1000+ files)
    // For normal testing encodetest/file_small.json (10 files)
    Blob sourceBlob =
        storage.get(BlobId.of(testConfiguration.getIngestbucket(), "encodetest/file_small.json"));

    List<BulkLoadFileModel> loadArray = new ArrayList<>();
    List<EncodeFileIn> inArray = new ArrayList<>();

    try (BufferedReader reader =
        new BufferedReader(Channels.newReader(sourceBlob.reader(), "UTF-8"))) {
      String line = null;
      while ((line = reader.readLine()) != null) {
        EncodeFileIn encodeFileIn = TestUtils.mapFromJson(line, EncodeFileIn.class);
        inArray.add(encodeFileIn);

        if (encodeFileIn.getFile_gs_path() != null) {
          loadArray.add(makeFileModel(encodeFileIn.getFile_gs_path()));
        }

        if (encodeFileIn.getFile_index_gs_path() != null) {
          loadArray.add(makeFileModel(encodeFileIn.getFile_index_gs_path()));
        }
      }
    }

    BulkLoadArrayRequestModel loadRequest =
        new BulkLoadArrayRequestModel()
            .loadArray(loadArray)
            .maxFailedFileLoads(0)
            .profileId(profileId)
            .loadTag("encodeFixture");

    BulkLoadArrayResultModel loadResult =
        dataRepoFixtures.bulkLoadArray(user, datasetId, loadRequest);

    Map<String, BulkLoadFileResultModel> resultMap = new HashMap<>();
    for (BulkLoadFileResultModel fileResult : loadResult.getLoadFileResults()) {
      resultMap.put(fileResult.getSourcePath(), fileResult);
    }

    try (GcsChannelWriter writer =
        new GcsChannelWriter(storage, testConfiguration.getIngestbucket(), loadData)) {
      for (EncodeFileIn encodeFileIn : inArray) {
        BulkLoadFileResultModel resultModel = resultMap.get(encodeFileIn.getFile_gs_path());
        String bamFileId = (resultModel == null) ? null : resultModel.getFileId();
        resultModel = resultMap.get(encodeFileIn.getFile_index_gs_path());
        String bamiFileId = (resultModel == null) ? null : resultModel.getFileId();
        EncodeFileOut encodeFileOut = new EncodeFileOut(encodeFileIn, bamFileId, bamiFileId);
        String fileLine = TestUtils.mapToJson(encodeFileOut);
        writer.writeLine(fileLine);
      }
    }

    return loadData;
  }

  public void deleteLoadFile(TestConfiguration.User user, String loadData) {
    String userToken = authService.getDirectAccessAuthToken(user.getEmail());
    Storage storage = dataRepoFixtures.getStorage(userToken);
    Blob targetBlob = storage.get(BlobId.of(testConfiguration.getIngestbucket(), loadData));
    targetBlob.delete();
  }

  private BulkLoadFileModel makeFileModel(String gspath) {
    return new BulkLoadFileModel().sourcePath(gspath).targetPath(URI.create(gspath).getPath());
  }
}
