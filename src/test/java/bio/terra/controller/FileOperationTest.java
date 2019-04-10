package bio.terra.controller;

import bio.terra.category.Connected;
import bio.terra.fixtures.ConnectedOperations;
import bio.terra.fixtures.JsonLoader;
import bio.terra.fixtures.Names;
import bio.terra.model.AccessMethod;
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
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Connected.class)
public class FileOperationTest {
    @Autowired private MockMvc mvc;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private JsonLoader jsonLoader;

    private ConnectedOperations connectedOperations;

    @Before
    public void setup() {
        connectedOperations = new ConnectedOperations(mvc, objectMapper, jsonLoader);
    }

    @After
    public void teardown() throws Exception {
        connectedOperations.teardown();
    }

    @Test
    public void testMetadataStep() throws Exception {
        // Simple test to verify the ingest file metadata step.
        // This is temporary, to test this "midway through" PR.

        StudySummaryModel studySummary = connectedOperations.createTestStudy("dataset-test-study.json");

        String targetPath = "/foo/bar/" + Names.randomizeName("file") + ".bam";

        FileLoadModel fileLoadModel = new FileLoadModel()
            .sourcePath("gs://bucket/file.bam")
            .description("test description")
            .mimeType("application/octet-stream")
            .targetPath(targetPath);


        String jsonRequest = objectMapper.writeValueAsString(fileLoadModel);
        String url = "/api/repository/v1/study/" + studySummary.getId() + "/file";
        MvcResult result = mvc.perform(post(url)
            .contentType(MediaType.APPLICATION_JSON)
            .content(jsonRequest))
            .andReturn();

        MockHttpServletResponse response = connectedOperations.validateJobModelAndWait(result);

        FileModel fileModel = connectedOperations.handleAsyncSuccessCase(response, FileModel.class);
        assertThat("description matches", fileModel.getDescription(), equalTo("test description"));
        assertThat("mime type matches", fileModel.getMimeType(), equalTo("application/octet-stream"));
        assertThat("access is gs", fileModel.getAccessMethods().get(0).getType(),
            equalTo(AccessMethod.TypeEnum.GS));
    }

}
