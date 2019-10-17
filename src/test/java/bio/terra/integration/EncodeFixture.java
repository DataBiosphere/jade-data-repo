package bio.terra.integration;

import bio.terra.service.filedata.google.firestore.EncodeFileIn;
import bio.terra.service.filedata.google.firestore.EncodeFileOut;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.integration.auth.AuthService;
import bio.terra.integration.configuration.TestConfiguration;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.iam.SamClientService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.WriteChannel;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ActiveProfiles;

import java.io.BufferedReader;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.UUID;

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

    // Create dataset, load files and tables. Create and return snapshot.
    // Steward owns dataset; custodian is custodian on dataset; reader has access to the snapshot.
    // TODO: add tearDownEncode
    public SnapshotSummaryModel setupEncode(
        TestConfiguration.User steward,
        TestConfiguration.User custodian,
        TestConfiguration.User reader) throws Exception {

        DatasetSummaryModel datasetSummary = dataRepoFixtures.createDataset(steward, "encodefiletest-dataset.json");
        String datasetId = datasetSummary.getId();

        // TODO: Fix use of SamClientService - see DR-494
        dataRepoFixtures.addDatasetPolicyMember(
            steward,
            datasetId,
            SamClientService.DataRepoRole.CUSTODIAN,
            custodian.getEmail());

        // Parse the input data and load the files; generate revised data file
        String stewardToken = authService.getDirectAccessAuthToken(steward.getEmail());
        Storage stewardStorage = dataRepoFixtures.getStorage(stewardToken);
        BillingProfileModel billingProfile = dataRepoFixtures.createBillingProfile(steward);
        String targetPath = loadFiles(datasetSummary.getId(), billingProfile.getId(), steward, stewardStorage);

        // Load the tables
        IngestRequestModel request = dataRepoFixtures.buildSimpleIngest(
            "file", targetPath, IngestRequestModel.StrategyEnum.APPEND);
        dataRepoFixtures.ingestJsonData(steward, datasetId, request);
        request = dataRepoFixtures.buildSimpleIngest(
            "donor", "encodetest/donor.json", IngestRequestModel.StrategyEnum.APPEND);
        dataRepoFixtures.ingestJsonData(steward, datasetId, request);

        // Delete the scratch blob
        Blob scratchBlob = stewardStorage.get(BlobId.of(testConfiguration.getIngestbucket(), targetPath));
        if (scratchBlob != null) {
            scratchBlob.delete();
        }

        // At this point, we have files and tabular data. Let's make a data snapshot!
        SnapshotSummaryModel snapshotSummary = dataRepoFixtures.createSnapshot(
            custodian, datasetSummary, "encodefiletest-snapshot.json");

        // TODO: Fix use of SamClientService - see DR-494
        dataRepoFixtures.addSnapshotPolicyMember(
            custodian,
            snapshotSummary.getId(),
            SamClientService.DataRepoRole.READER,
            reader.getEmail());

        // We wait here for SAM to sync. We expect this to take 5 minutes. It can take more as recent
        // issues have shown. We make a BigQuery request as the test to see that READER has access.
        // We need to get the snapshot, rather than the snapshot summary in order to make a query.
        // TODO: Add dataProject to SnapshotSummaryModel?
        SnapshotModel snapshotModel = dataRepoFixtures.getSnapshot(custodian, snapshotSummary.getId());
        String readerToken = authService.getDirectAccessAuthToken(reader.getEmail());
        BigQuery bigQueryReader = BigQueryFixtures.getBigQuery(testConfiguration.getGoogleProjectId(), readerToken);
        BigQueryFixtures.hasAccess(bigQueryReader, snapshotModel.getDataProject(), snapshotModel.getName());

        return snapshotSummary;
    }

    private String loadFiles(
        String datasetId,
        String profileId,
        TestConfiguration.User user,
        Storage storage) throws Exception {
        // Open the source data from the bucket
        // Open target data in bucket
        // Read one line at a time - unpack into pojo
        // Ingest the files, substituting the file ids
        // Generate JSON and write the line to scratch
        String targetPath = "scratch/file" + UUID.randomUUID().toString() + ".json";

        // For a bigger test use encodetest/file.json (1000+ files)
        // For normal testing encodetest/file_small.json (10 files)
        Blob sourceBlob = storage.get(
            BlobId.of(testConfiguration.getIngestbucket(), "encodetest/file_small.json"));

        BlobInfo targetBlobInfo = BlobInfo
            .newBuilder(BlobId.of(testConfiguration.getIngestbucket(), targetPath))
            .build();

        try (WriteChannel writer = storage.writer(targetBlobInfo);
             BufferedReader reader = new BufferedReader(Channels.newReader(sourceBlob.reader(), "UTF-8"))) {

            String line = null;
            while ((line = reader.readLine()) != null) {
                EncodeFileIn encodeFileIn = objectMapper.readValue(line, EncodeFileIn.class);

                String bamFileId = null;
                String bamiFileId = null;

                if (encodeFileIn.getFile_gs_path() != null) {
                    bamFileId = loadOneFile(user, datasetId, profileId, encodeFileIn.getFile_gs_path());
                }

                if (encodeFileIn.getFile_index_gs_path() != null) {
                    bamiFileId = loadOneFile(user, datasetId, profileId, encodeFileIn.getFile_index_gs_path());
                }

                EncodeFileOut encodeFileOut = new EncodeFileOut(encodeFileIn, bamFileId, bamiFileId);
                String fileLine = objectMapper.writeValueAsString(encodeFileOut) + "\n";
                writer.write(ByteBuffer.wrap(fileLine.getBytes("UTF-8")));
            }
        }

        return targetPath;
    }

    private String loadOneFile(
        TestConfiguration.User user,
        String datasetId,
        String profileId,
        String gsPath) throws Exception {
        String filePath = URI.create(gsPath).getPath();
        FileModel fileModel = dataRepoFixtures.ingestFile(user, datasetId, profileId, gsPath, filePath);
        return fileModel.getFileId();
    }

}
