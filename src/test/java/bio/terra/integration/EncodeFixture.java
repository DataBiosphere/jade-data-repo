package bio.terra.integration;

import bio.terra.filesystem.EncodeFileIn;
import bio.terra.filesystem.EncodeFileOut;
import bio.terra.fixtures.JsonLoader;
import bio.terra.integration.auth.AuthService;
import bio.terra.integration.configuration.TestConfiguration;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.FSObjectModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.service.SamClientService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ActiveProfiles;

import java.io.BufferedReader;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.UUID;

@Profile("integrationtest")
@ActiveProfiles({"google", "integrationtest"})
@Component
public class EncodeFixture {
    private static final Logger logger = LoggerFactory.getLogger(EncodeFixture.class);

    @Autowired private JsonLoader jsonLoader;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private DataRepoClient dataRepoClient;
    @Autowired private DataRepoFixtures dataRepoFixtures;
    @Autowired private AuthService authService;
    @Autowired private TestConfiguration testConfig;

    // Create study, load files and tables. Create and return dataset.
    // Steward owns study; custodian is custodian on study; reader has access to the dataset.
    public DatasetSummaryModel setupEncode(
        TestConfiguration.User steward,
        TestConfiguration.User custodian,
        TestConfiguration.User reader) throws Exception {

        StudySummaryModel studySummary = dataRepoFixtures.createStudy(steward, "encodefiletest-study.json");
        String studyId = studySummary.getId();

        // TODO: Fix use of SamClientService - see DR-494
        dataRepoFixtures.addStudyPolicyMember(
            steward,
            studyId,
            SamClientService.DataRepoRole.CUSTODIAN,
            custodian.getEmail());

        // Parse the input data and load the files; generate revised data file
        String stewardToken = authService.getDirectAccessAuthToken(steward.getEmail());
        Storage stewardStorage = dataRepoFixtures.getStorage(stewardToken);
        String targetPath = loadFiles(studySummary.getId(), steward, stewardStorage);

        // Load the tables
        dataRepoFixtures.ingestJsonData(steward, studyId, "file", targetPath);
        dataRepoFixtures.ingestJsonData(steward, studyId, "donor", "encodetest/donor.json");

        // Delete the scratch blob
        Blob scratchBlob = stewardStorage.get(BlobId.of(testConfig.getIngestbucket(), targetPath));
        if (scratchBlob != null) {
            scratchBlob.delete();
        }

        // At this point, we have files and tabular data. Let's make a dataset!
        DatasetSummaryModel datasetSummary = dataRepoFixtures.createDataset(
            custodian, studySummary, "encodefiletest-dataset.json");

        // TODO: Fix use of SamClientService - see DR-494
        dataRepoFixtures.addDatasetPolicyMember(
            custodian,
            datasetSummary.getId(),
            SamClientService.DataRepoRole.READER,
            reader.getEmail());

        return datasetSummary;
    }

    private String loadFiles(String studyId, TestConfiguration.User user, Storage storage) throws Exception {
        // Open the source data from the bucket
        // Open target data in bucket
        // Read one line at a time - unpack into pojo
        // Ingest the files, substituting the file ids
        // Generate JSON and write the line to scratch
        String targetPath = "scratch/file" + UUID.randomUUID().toString() + ".json";

        // For a bigger test use encodetest/file.json (1000+ files)
        // For normal testing encodetest/file_small.json (10 files)
        Blob sourceBlob = storage.get(
            BlobId.of(testConfig.getIngestbucket(), "encodetest/file_small.json"));

        BlobInfo targetBlobInfo = BlobInfo
            .newBuilder(BlobId.of(testConfig.getIngestbucket(), targetPath))
            .build();

        try (WriteChannel writer = storage.writer(targetBlobInfo);
             BufferedReader reader = new BufferedReader(Channels.newReader(sourceBlob.reader(), "UTF-8"))) {

            String line = null;
            while ((line = reader.readLine()) != null) {
                EncodeFileIn encodeFileIn = objectMapper.readValue(line, EncodeFileIn.class);

                String bamFileId = null;
                String bamiFileId = null;

                if (encodeFileIn.getFile_gs_path() != null) {
                    FSObjectModel bamFile = dataRepoFixtures.ingestFile(
                        user, studyId, encodeFileIn.getFile_gs_path(), targetPath);
                    bamFileId = bamFile.getObjectId();
                }

                if (encodeFileIn.getFile_index_gs_path() != null) {
                    FSObjectModel bamiFile = dataRepoFixtures.ingestFile(
                        user, studyId, encodeFileIn.getFile_index_gs_path(), targetPath);
                    bamiFileId = bamiFile.getObjectId();
                }

                EncodeFileOut encodeFileOut = new EncodeFileOut(encodeFileIn, bamFileId, bamiFileId);
                String fileLine = objectMapper.writeValueAsString(encodeFileOut) + "\n";
                writer.write(ByteBuffer.wrap(fileLine.getBytes("UTF-8")));
            }
        }

        return targetPath;
    }

}
