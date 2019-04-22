package bio.terra.controller;

import bio.terra.category.Connected;
import bio.terra.fixtures.ConnectedOperations;
import bio.terra.fixtures.JsonLoader;
import bio.terra.integration.DataRepoConfiguration;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.service.SamClientService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.io.BufferedReader;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("google")
@Category(Connected.class)
public class EncodeFileTest {
    @Autowired private MockMvc mvc;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private JsonLoader jsonLoader;
    @Autowired private DataRepoConfiguration dataRepoConfiguration;
    @Autowired private Storage storage;

    @MockBean
    private SamClientService samService;

    private ConnectedOperations connectedOperations;

    @Before
    public void setup() throws Exception {
        // Setup mock sam service
        doNothing().when(samService).createStudyResource(any(), any());
        when(samService.createDatasetResource(any(), any())).thenReturn("hi");
        doNothing().when(samService).deleteDatasetResource(any(), any());
        doNothing().when(samService).deleteStudyResource(any(), any());

        connectedOperations = new ConnectedOperations(mvc, objectMapper, jsonLoader);
    }

    @After
    public void teardown() throws Exception {
        connectedOperations.teardown();
    }

    @Test
    public void encodeFileTest() throws Exception {
        StudySummaryModel studySummary = connectedOperations.createTestStudy("encode-test-study.json");
        String gsPath = loadFiles(studySummary.getId());

        IngestRequestModel ingestRequest = new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .table("file")
            .path(gsPath);

        String jsonRequest = objectMapper.writeValueAsString(ingestRequest);
        String url = "/api/repository/v1/studies/" + studySummary.getId() + "/ingest";

        MvcResult result = mvc.perform(post(url)
            .contentType(MediaType.APPLICATION_JSON)
            .content(jsonRequest))
            .andReturn();

        MockHttpServletResponse response = result.getResponse();
        assertThat("load files success", HttpStatus.valueOf(response.getStatus()), equalTo(HttpStatus.OK));

        // Load donor success
        ingestRequest
            .table("donor")
            .path("gs://" + dataRepoConfiguration.getIngestbucket() + "/encodetest/donor.json");

        jsonRequest = objectMapper.writeValueAsString(ingestRequest);
        result = mvc.perform(post(url)
            .contentType(MediaType.APPLICATION_JSON)
            .content(jsonRequest))
            .andReturn();
        response = result.getResponse();
        assertThat("load donor success", HttpStatus.valueOf(response.getStatus()), equalTo(HttpStatus.OK));
    }

    private String loadFiles(String studyId) throws Exception {
        // Open the source data from the bucket
        // Open target data in bucket
        // Read one line at a time - unpack into pojo
        // Ingest the files, substituting the file ids
        // Generate JSON and write the line to scratch
        String targetPath = "scratch/file" + UUID.randomUUID().toString() + ".json";

        Blob sourceBlob = storage.get(BlobId.of(dataRepoConfiguration.getIngestbucket(), "encodetest/file.json"));
        assertNotNull("source blob not null", sourceBlob);

        BlobInfo targetBlobInfo = BlobInfo
            .newBuilder(BlobId.of(dataRepoConfiguration.getIngestbucket(), targetPath))
            .build();

        try (WriteChannel writer = storage.writer(targetBlobInfo);
             BufferedReader reader = new BufferedReader(Channels.newReader(sourceBlob.reader(), "UTF-8"))) {

            String line = null;
            while ((line = reader.readLine()) != null) {
                EncodeFile encodeFile = objectMapper.readValue(line, EncodeFile.class);

                FileLoadModel fileLoadModel = makeFileLoadModel(encodeFile.getFile_ref());
                FileModel bamFile = connectedOperations.ingestFileSuccess(studyId, fileLoadModel);

                fileLoadModel = makeFileLoadModel(encodeFile.getFile_index_ref());
                FileModel bamiFile = connectedOperations.ingestFileSuccess(studyId, fileLoadModel);

                encodeFile
                    .file_ref(bamFile.getFileId())
                    .file_index_ref(bamiFile.getFileId());

                String fileLine = objectMapper.writeValueAsString(encodeFile) + "\n";
                writer.write(ByteBuffer.wrap(fileLine.getBytes()));
            }
        }

        return "gs://" + dataRepoConfiguration.getIngestbucket() + "/" + targetPath;
    }

    private FileLoadModel makeFileLoadModel(String gspath) throws Exception {
        URI uri = URI.create(gspath);
        FileLoadModel fileLoadModel = new FileLoadModel()
            .sourcePath(gspath)
            .description(null)
            .mimeType("application/octet-string")
            .targetPath(uri.getPath());

        return fileLoadModel;
    }

}
