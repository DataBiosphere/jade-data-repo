package bio.terra.filesystem;

import bio.terra.category.Connected;
import bio.terra.configuration.ConnectedTestConfiguration;
import bio.terra.fixtures.ConnectedOperations;
import bio.terra.fixtures.JsonLoader;
import bio.terra.fixtures.Names;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FSObjectModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.resourcemanagement.service.google.GoogleResourceConfiguration;
import bio.terra.service.DrsIdService;
import bio.terra.service.SamClientService;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.net.URI;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class FileOperationTest {
    @Autowired private MockMvc mvc;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private JsonLoader jsonLoader;
    @Autowired private ConnectedTestConfiguration testConfig;
    @Autowired private DrsIdService drsService;
    @Autowired private GoogleResourceConfiguration googleResourceConfiguration;
    @Autowired private ConnectedOperations connectedOperations;

    @MockBean
    private SamClientService samService;

    private int validFileCounter;

    @Before
    public void setup() throws Exception {
        // Setup mock sam service
        connectedOperations.stubOutSamCalls(samService);
        validFileCounter = 0;
    }

    @After
    public void teardown() throws Exception {
        connectedOperations.teardown();
    }

    private static String testDescription = "test file description";
    private static String testMimeType = "application/pdf";
    private static String testPdfFile = "File Design Notes.pdf";

    @Test
    public void fileOperationsTest() throws Exception {
        String coreBillingAccountId = googleResourceConfiguration.getCoreBillingAccount();
        BillingProfileModel profileModel = connectedOperations.getOrCreateProfileForAccount(coreBillingAccountId);
        DatasetSummaryModel datasetSummary = connectedOperations.createDatasetWithFlight(profileModel,
            "snapshot-test-dataset.json");
        FileLoadModel fileLoadModel = makeFileLoad(profileModel.getId());

        FSObjectModel fileModel = connectedOperations.ingestFileSuccess(datasetSummary.getId(), fileLoadModel);
        assertThat("file path matches", fileModel.getPath(), equalTo(fileLoadModel.getTargetPath()));

        // lookup the file we just created
        String url = "/api/repository/v1/datasets/" + datasetSummary.getId() + "/files/" + fileModel.getObjectId();
        MvcResult result = mvc.perform(get(url))
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .andReturn();
        MockHttpServletResponse response = result.getResponse();
        assertThat("Lookup file succeeds", HttpStatus.valueOf(response.getStatus()), equalTo(HttpStatus.OK));

        FSObjectModel lookupModel = objectMapper.readValue(response.getContentAsString(), FSObjectModel.class);
        assertTrue("Ingest file equals lookup file", lookupModel.equals(fileModel));

        // Error: Duplicate target file
        ErrorModel errorModel = connectedOperations.ingestFileFailure(datasetSummary.getId(), fileLoadModel);
        assertThat("duplicate file error", errorModel.getMessage(),
            containsString("already exists"));

        // Lookup the file by path
        url = "/api/repository/v1/datasets/" + datasetSummary.getId() +
            "/filesystem/objects?path=" + fileModel.getPath();
        result = mvc.perform(get(url))
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .andReturn();
        response = result.getResponse();
        assertThat("Lookup file by path succeeds", HttpStatus.valueOf(response.getStatus()), equalTo(HttpStatus.OK));
        lookupModel = objectMapper.readValue(response.getContentAsString(), FSObjectModel.class);
        assertTrue("Ingest file equals lookup file", lookupModel.equals(fileModel));

        // Delete the file and we should be able to create it successfully again
        connectedOperations.deleteTestFile(datasetSummary.getId(), fileModel.getObjectId());
        fileModel = connectedOperations.ingestFileSuccess(datasetSummary.getId(), fileLoadModel);
        assertThat("file path matches", fileModel.getPath(), equalTo(fileLoadModel.getTargetPath()));

        // Error: Non-existent source file
        String badfile = "/I am not a file";
        URI uribadfile = new URI("gs",
            testConfig.getIngestbucket(),
            badfile,
            null,
            null);
        String badPath = "/dd/files/" + Names.randomizeName("dir") + badfile;

        fileLoadModel = new FileLoadModel()
            .profileId(profileModel.getId())
            .sourcePath(uribadfile.toString())
            .description(testDescription)
            .mimeType(testMimeType)
            .targetPath(badPath);

        errorModel = connectedOperations.ingestFileFailure(datasetSummary.getId(), fileLoadModel);
        assertThat("source file does not exist", errorModel.getMessage(),
            containsString("file not found"));

        // Error: Invalid gs path - case 1: not gs
        fileLoadModel = new FileLoadModel()
            .profileId(profileModel.getId())
            .sourcePath("http://jade_notabucket/foo/bar.txt")
            .description(testDescription)
            .mimeType(testMimeType)
            .targetPath(makeValidUniqueFilePath());

        errorModel = connectedOperations.ingestFileFailure(datasetSummary.getId(), fileLoadModel);
        assertThat("Not a gs schema", errorModel.getMessage(),
            containsString("not a gs"));

        // Error: Invalid gs path - case 2: invalid bucket name
        fileLoadModel = new FileLoadModel()
            .profileId(profileModel.getId())
            .sourcePath("gs://jade_notabucket:1234/foo/bar.txt")
            .description(testDescription)
            .mimeType(testMimeType)
            .targetPath(makeValidUniqueFilePath());

        errorModel = connectedOperations.ingestFileFailure(datasetSummary.getId(), fileLoadModel);
        assertThat("Invalid bucket name", errorModel.getMessage(),
            containsString("Invalid bucket name"));

        // Error: Invalid gs path - case 3: no bucket or path
        fileLoadModel = new FileLoadModel()
            .profileId(profileModel.getId())
            .sourcePath("gs:///")
            .description(testDescription)
            .mimeType(testMimeType)
            .targetPath(makeValidUniqueFilePath());

        errorModel = connectedOperations.ingestFileFailure(datasetSummary.getId(), fileLoadModel);
        assertThat("No bucket or path", errorModel.getMessage(),
            containsString("gs path"));
    }

    private String makeValidUniqueFilePath() {
        validFileCounter++;
        return String.format("/dd/files/foo/ValidFileName%d.pdf", validFileCounter);
    }

    private FileLoadModel makeFileLoad(String profileId) throws Exception {
        String targetDir = Names.randomizeName("dir");
        URI uri = new URI("gs",
            testConfig.getIngestbucket(),
            "/files/" + testPdfFile,
            null,
            null);
        String targetPath = "/dd/files/" + targetDir + "/" + testPdfFile;

        return new FileLoadModel()
            .sourcePath(uri.toString())
            .description(testDescription)
            .mimeType(testMimeType)
            .targetPath(targetPath)
            .profileId(profileId);
    }

}
