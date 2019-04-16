package bio.terra.controller;

import bio.terra.category.Connected;
import bio.terra.fixtures.ConnectedOperations;
import bio.terra.fixtures.JsonLoader;
import bio.terra.fixtures.Names;
import bio.terra.integration.DataRepoConfiguration;
import bio.terra.model.ErrorModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
import bio.terra.model.StudySummaryModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import java.net.URI;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("google")
@Category(Connected.class)
public class FileOperationTest {
    @Autowired private MockMvc mvc;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private JsonLoader jsonLoader;
    @Autowired private DataRepoConfiguration dataRepoConfiguration;

    private ConnectedOperations connectedOperations;

    @Before
    public void setup() {
        connectedOperations = new ConnectedOperations(mvc, objectMapper, jsonLoader);
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
        URI uri = new URI("gs",
            dataRepoConfiguration.getIngestbucket(),
            "/files/" + testPdfFile,
            null,
            null);
        String targetPath = "/dd/files/" + Names.randomizeName("dir") + "/" + testPdfFile;

        StudySummaryModel studySummary = connectedOperations.createTestStudy("dataset-test-study.json");

        FileLoadModel fileLoadModel = new FileLoadModel()
            .sourcePath(uri.toString())
            .description(testDescription)
            .mimeType(testMimeType)
            .targetPath(targetPath);

        FileModel fileModel = connectedOperations.ingestFileSuccess(studySummary.getId(), fileLoadModel);
        assertThat("file name matches", fileModel.getName(), equalTo(testPdfFile));

        // Error: Duplicate target file
        ErrorModel errorModel = connectedOperations.ingestFileFailure(studySummary.getId(), fileLoadModel);
        assertThat("duplicate file error", errorModel.getMessage(),
            containsString("already exists"));

/*
These test cases won't work until we fix the deserialization bug

        // Error: Non-existent source file

        // Error: Invalid gs path
        //  case: not a gs:
        //  case: port specified
        //  case: no bucket or path
*/
    }


}
