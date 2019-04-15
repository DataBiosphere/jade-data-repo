package bio.terra.controller;

import bio.terra.category.Connected;
import bio.terra.fixtures.ConnectedOperations;
import bio.terra.fixtures.JsonLoader;
import bio.terra.fixtures.Names;
import bio.terra.integration.DataRepoConfiguration;
import bio.terra.model.AccessMethod;
import bio.terra.model.Checksum;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
import bio.terra.model.StudySummaryModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.net.URI;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
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
    public void happyPath() throws Exception {
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

        String jsonRequest = objectMapper.writeValueAsString(fileLoadModel);
        String url = "/api/repository/v1/studies/" + studySummary.getId() + "/file";
        MvcResult result = mvc.perform(post(url)
            .contentType(MediaType.APPLICATION_JSON)
            .content(jsonRequest))
            .andReturn();

        MockHttpServletResponse response = connectedOperations.validateJobModelAndWait(result);

        FileModel fileModel = connectedOperations.handleAsyncSuccessCase(response, FileModel.class);
        assertThat("description matches", fileModel.getDescription(), equalTo(testDescription));
        assertThat("mime type matches", fileModel.getMimeType(), equalTo(testMimeType));
        assertThat("access is gs", fileModel.getAccessMethods().get(0).getType(),
            equalTo(AccessMethod.TypeEnum.GS));
        assertThat("file name matches", fileModel.getName(), equalTo(testPdfFile));

        for (Checksum checksum : fileModel.getChecksums()) {
            assertTrue("valid checksum type",
                (StringUtils.equals(checksum.getType(), "crc32c") ||
                    StringUtils.equals(checksum.getType(), "md5")));
        }
    }

}
