package bio.terra.filesystem;

import bio.terra.category.Connected;
import bio.terra.configuration.ConnectedTestConfiguration;
import bio.terra.fixtures.ConnectedOperations;
import bio.terra.fixtures.JsonLoader;
import bio.terra.model.DRSBundle;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.FSObjectModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.IngestRequestModel;
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
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class EncodeFileTest {
    @Autowired private MockMvc mvc;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private JsonLoader jsonLoader;
    @Autowired private Storage storage;
    @Autowired private BigQuery bigQuery;
    @Autowired private String bigQueryProjectId;
    @Autowired private ConnectedTestConfiguration testConfig;

    private static final String ID_GARBAGE = "GARBAGE";

    @MockBean
    private SamClientService samService;

    private ConnectedOperations connectedOperations;

    @Before
    public void setup() throws Exception {
        // Setup mock sam service
        ConnectedOperations.stubOutSamCalls(samService);

        connectedOperations = new ConnectedOperations(mvc, objectMapper, jsonLoader);
    }

    @After
    public void teardown() throws Exception {
//        connectedOperations.teardown();
    }

    // NOTES ABOUT THIS TEST: this test requires create access to the jade-testdata bucket in order to
    // re-write the json source data replacing the gs paths with the Jade object id.
    @Test
    public void encodeFileTest() throws Exception {
        StudySummaryModel studySummary = connectedOperations.createTestStudy("encodefiletest-study.json");
        String targetPath = loadFiles(studySummary.getId(), false, false);
        String gsPath = "gs://" + testConfig.getIngestbucket() + "/" + targetPath;

        IngestRequestModel ingestRequest = new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .table("file")
            .path(gsPath);

        connectedOperations.ingestTableSuccess(studySummary.getId(), ingestRequest);

        // Delete the scratch blob
        Blob scratchBlob = storage.get(BlobId.of(testConfig.getIngestbucket(), targetPath));
        if (scratchBlob != null) {
            scratchBlob.delete();
        }

        // Load donor success
        ingestRequest
            .table("donor")
            .path("gs://" + testConfig.getIngestbucket() + "/encodetest/donor.json");

        connectedOperations.ingestTableSuccess(studySummary.getId(), ingestRequest);

        // At this point, we have files and tabular data. Let's make a dataset!

        MockHttpServletResponse response = connectedOperations.launchCreateDataset(
            studySummary, "encodefiletest-datasnapshot.json", "");
        DatasetSummaryModel datasetSummary = connectedOperations.handleCreateDatasetSuccessCase(response);

        String datasetFileId = getFileRefIdFromDataset(datasetSummary.getName());

        // Try to delete a file with a dependency
        MvcResult result = mvc.perform(
            delete("/api/repository/v1/studies/" + studySummary.getId() + "/files/" + datasetFileId))
            .andReturn();
        response = connectedOperations.validateJobModelAndWait(result);
        assertThat(response.getStatus(), equalTo(HttpStatus.BAD_REQUEST.value()));

        ErrorModel errorModel = connectedOperations.handleAsyncFailureCase(response);
        assertThat("correct dependency error message",
            errorModel.getMessage(), containsString("used by at least one dataset"));
    }

    @Test
    public void encodeFileBadFileId() throws Exception {
        StudySummaryModel studySummary = connectedOperations.createTestStudy("encodefiletest-study.json");
        String targetPath = loadFiles(studySummary.getId(), true, false);
        String gsPath = "gs://" + testConfig.getIngestbucket() + "/" + targetPath;

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

        ErrorModel ingestError = connectedOperations.handleAsyncFailureCase(response);
        assertThat("correctly found bad file id",
            ingestError.getMessage(), containsString("Invalid file ids found"));

        List<String> errorDetails = ingestError.getErrorDetail();
        assertNotNull("Error details were returned", errorDetails);
        assertThat("Bad id was returned in details", errorDetails.get(0), endsWith(ID_GARBAGE));

        // Delete the scratch blob
        Blob scratchBlob = storage.get(BlobId.of(testConfig.getIngestbucket(), targetPath));
        if (scratchBlob != null) {
            scratchBlob.delete();
        }
    }

    @Test
    public void encodeFileBadRowTest() throws Exception {
        StudySummaryModel studySummary = connectedOperations.createTestStudy("encodefiletest-study.json");
        String targetPath = loadFiles(studySummary.getId(), false, true);
        String gsPath = "gs://" + testConfig.getIngestbucket() + "/" + targetPath;

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

        ErrorModel ingestError = connectedOperations.handleAsyncFailureCase(response);
        assertThat("correctly found bad row",
            ingestError.getMessage(), equalTo("Ingest failed with 2 errors - see error details"));

        List<String> errorDetails = ingestError.getErrorDetail();
        assertNotNull("Error details were returned", errorDetails);
        assertThat("Big query returned in details 0", errorDetails.get(0),
            equalTo("BigQueryError: reason=invalid message=Error while reading data, error message: " +
                "JSON table encountered too many errors, giving up. Rows: 1; errors: 1. Please look into the " +
                "errors[] collection for more details."));
        assertThat("Big query returned in details 1", errorDetails.get(1),
            equalTo("BigQueryError: reason=invalid message=Error while reading data, error message: " +
                "JSON parsing error in row starting at position 0: Parser terminated before end of string"));

        // Delete the scratch blob
        Blob scratchBlob = storage.get(BlobId.of(testConfig.getIngestbucket(), targetPath));
        if (scratchBlob != null) {
            scratchBlob.delete();
        }
    }

    // TODO: Bundle testing - complete when we have a method for getting a bundle id
    private void getOneBundle(String bundleId) throws Exception {
        String url = "/ga4gh/drs/v1/bundles/" + bundleId;
        MvcResult result = mvc.perform(get(url)).andReturn();
        MockHttpServletResponse response = result.getResponse();
        if (response.getStatus() == 200) {
            DRSBundle bundle = objectMapper.readValue(response.getContentAsString(), DRSBundle.class);
            System.out.println(bundle);
        }
    }

    private String loadFiles(String studyId, boolean insertBadId, boolean insertBadRow) throws Exception {
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
        assertNotNull("source blob not null", sourceBlob);

        BlobInfo targetBlobInfo = BlobInfo
            .newBuilder(BlobId.of(testConfig.getIngestbucket(), targetPath))
            .build();

        try (WriteChannel writer = storage.writer(targetBlobInfo);
             BufferedReader reader = new BufferedReader(Channels.newReader(sourceBlob.reader(), "UTF-8"))) {

            boolean badIdInserted = false;
            boolean badRowInserted = false;
            String line = null;
            while ((line = reader.readLine()) != null) {
                EncodeFileIn encodeFileIn = objectMapper.readValue(line, EncodeFileIn.class);

                String bamFileId = null;
                String bamiFileId = null;

                if (encodeFileIn.getFile_gs_path() != null) {
                    FileLoadModel fileLoadModel = makeFileLoadModel(encodeFileIn.getFile_gs_path());
                    FSObjectModel bamFile = connectedOperations.ingestFileSuccess(studyId, fileLoadModel);
                    // Fault insertion on request: we corrupt one id if requested to do so.
                    if (insertBadId && !badIdInserted) {
                        bamFileId = bamFile.getObjectId() + ID_GARBAGE;
                        badIdInserted = true;
                    } else {
                        bamFileId = bamFile.getObjectId();
                    }

                }

                if (encodeFileIn.getFile_index_gs_path() != null) {
                    FileLoadModel fileLoadModel = makeFileLoadModel(encodeFileIn.getFile_index_gs_path());
                    FSObjectModel bamiFile = connectedOperations.ingestFileSuccess(studyId, fileLoadModel);
                    bamiFileId = bamiFile.getObjectId();
                }

                EncodeFileOut encodeFileOut = new EncodeFileOut(encodeFileIn, bamFileId, bamiFileId);
                String fileLine;
                if (insertBadRow && !badRowInserted) {
                    fileLine = "{\"fribbitz\";\"ABCDEFG\"}\n";
                } else {
                    fileLine = objectMapper.writeValueAsString(encodeFileOut) + "\n";
                }
                writer.write(ByteBuffer.wrap(fileLine.getBytes("UTF-8")));
            }
        }

        return targetPath;
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
