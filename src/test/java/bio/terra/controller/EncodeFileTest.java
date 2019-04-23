package bio.terra.controller;

import bio.terra.category.Connected;
import bio.terra.fixtures.ConnectedOperations;
import bio.terra.fixtures.JsonLoader;
import bio.terra.integration.DataRepoConfiguration;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.pdao.exception.PdaoException;
import bio.terra.service.SamClientService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.WriteChannel;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import org.apache.commons.lang3.StringUtils;
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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
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
    @Autowired private BigQuery bigQuery;
    @Autowired private String bigQueryProjectId;

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
        StudySummaryModel studySummary = connectedOperations.createTestStudy("encodefiletest-study.json");
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
        MockHttpServletResponse response = connectedOperations.validateJobModelAndWait(result);

        IngestResponseModel ingestResponse =
            connectedOperations.handleAsyncSuccessCase(response, IngestResponseModel.class);
        // TODO: remove
        System.out.println(ingestResponse);

        // Load donor success
        ingestRequest
            .table("donor")
            .path("gs://" + dataRepoConfiguration.getIngestbucket() + "/encodetest/donor.json");

        jsonRequest = objectMapper.writeValueAsString(ingestRequest);
        result = mvc.perform(post(url)
            .contentType(MediaType.APPLICATION_JSON)
            .content(jsonRequest))
            .andReturn();
        response = connectedOperations.validateJobModelAndWait(result);

        ingestResponse = connectedOperations.handleAsyncSuccessCase(response, IngestResponseModel.class);
        // TODO: remove
        System.out.println(ingestResponse);

        // At this point, we have files and tabular data. Let's make a dataset!

        response = connectedOperations.launchCreateDataset(studySummary, "encodefiletest-dataset.json", "");
        DatasetSummaryModel datasetSummary = connectedOperations.handleCreateDatasetSuccessCase(response);

        String datasetFileId = getFileRefIdFromDataset(datasetSummary.getName());

        // Try to delete a file with a dependency
        result = mvc.perform(
            delete("/api/repository/v1/studies/" + studySummary.getId() + "/files/" + datasetFileId))
            .andReturn();
        response = connectedOperations.validateJobModelAndWait(result);
        assertThat(response.getStatus(), equalTo(HttpStatus.BAD_REQUEST.value()));

        ErrorModel errorModel = connectedOperations.handleAsyncFailureCase(response);
        assertThat("correct dependency error message",
            errorModel.getMessage(), containsString("used by at least one dataset"));
    }

    private String loadFiles(String studyId) throws Exception {
        // Open the source data from the bucket
        // Open target data in bucket
        // Read one line at a time - unpack into pojo
        // Ingest the files, substituting the file ids
        // Generate JSON and write the line to scratch
        String targetPath = "scratch/file" + UUID.randomUUID().toString() + ".json";

        // For a bigger test use encodetest/file.json (1000+ files)
        // For normal testing encodetest/file_small.json (10 files)
        Blob sourceBlob = storage.get(
            BlobId.of(dataRepoConfiguration.getIngestbucket(), "encodetest/file_small.json"));
        assertNotNull("source blob not null", sourceBlob);

        BlobInfo targetBlobInfo = BlobInfo
            .newBuilder(BlobId.of(dataRepoConfiguration.getIngestbucket(), targetPath))
            .build();

        try (WriteChannel writer = storage.writer(targetBlobInfo);
             BufferedReader reader = new BufferedReader(Channels.newReader(sourceBlob.reader(), "UTF-8"))) {

            String line = null;
            while ((line = reader.readLine()) != null) {
                EncodeFileIn encodeFileIn = objectMapper.readValue(line, EncodeFileIn.class);

                String bamFileId = null;
                String bamiFileId = null;

                if (encodeFileIn.getFile_gs_path() != null) {
                    FileLoadModel fileLoadModel = makeFileLoadModel(encodeFileIn.getFile_gs_path());
                    FileModel bamFile = connectedOperations.ingestFileSuccess(studyId, fileLoadModel);
                    bamFileId = bamFile.getFileId();
                }

                if (encodeFileIn.getFile_index_gs_path() != null) {
                    FileLoadModel fileLoadModel = makeFileLoadModel(encodeFileIn.getFile_index_gs_path());
                    FileModel bamiFile = connectedOperations.ingestFileSuccess(studyId, fileLoadModel);
                    bamiFileId = bamiFile.getFileId();
                }

                EncodeFileOut encodeFileOut = new EncodeFileOut(encodeFileIn, bamFileId, bamiFileId);
                String fileLine = objectMapper.writeValueAsString(encodeFileOut) + "\n";
                writer.write(ByteBuffer.wrap(fileLine.getBytes("UTF-8")));
            }
        }

        return "gs://" + dataRepoConfiguration.getIngestbucket() + "/" + targetPath;
    }

    private String getFileRefIdFromDataset(String datasetName) {
        StringBuilder builder = new StringBuilder()
            .append("SELECT file_ref FROM `")
            .append(bigQueryProjectId)
            .append('.')
            .append(datasetName)
            .append(".file` AS T")
            .append(" WHERE T.file_ref IS NOT NULL LIMIT 1");

        String sql = builder.toString();
        try {
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
            TableResult result = bigQuery.query(queryConfig);
            FieldValueList row = result.iterateAll().iterator().next();
            FieldValue idValue = row.get(0);
            String drsUri = idValue.getStringValue();
            // Simple-minded way to grab the file id.
            String[] drsParts = StringUtils.split(drsUri, '_');
            return drsParts[drsParts.length - 1];
        } catch (InterruptedException ie) {
            throw new PdaoException("get file ref id from dataset unexpectedly interrupted", ie);
        }
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
