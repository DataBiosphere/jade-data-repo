package bio.terra.controller;

import bio.terra.category.Connected;
import bio.terra.fixtures.ConnectedOperations;
import bio.terra.fixtures.JsonLoader;
import bio.terra.fixtures.Names;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.DrDatasetRequestModel;
import bio.terra.model.DrDatasetSummaryModel;
import bio.terra.service.SamClientService;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class DrDatasetConnectedTest {
    @Autowired private MockMvc mvc;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private JsonLoader jsonLoader;
    @MockBean private SamClientService samService;

    @Test
    public void testCreateOmopDataset() throws Exception {
        ConnectedOperations.stubOutSamCalls(samService);

        DrDatasetRequestModel datasetRequest =
            jsonLoader.loadObject("it-dataset-omop.json", DrDatasetRequestModel.class);
        datasetRequest.name(Names.randomizeName(datasetRequest.getName()));

        MvcResult result = mvc.perform(post("/api/repository/v1/datasets")
            .contentType(MediaType.APPLICATION_JSON)
            .content(objectMapper.writeValueAsString(datasetRequest)))
            .andExpect(status().isCreated())
            .andReturn();
        MockHttpServletResponse response = result.getResponse();
        assertThat("create omop dataset successfully",
            response.getStatus(), equalTo(HttpStatus.CREATED.value()));
        DrDatasetSummaryModel datasetSummaryModel =
            objectMapper.readValue(response.getContentAsString(), DrDatasetSummaryModel.class);

        result = mvc.perform(delete("/api/repository/v1/datasets/" + datasetSummaryModel.getId()))
            .andReturn();
        response = result.getResponse();
        assertThat("delete omop dataset successfully", response.getStatus(), equalTo(HttpStatus.OK.value()));
        DeleteResponseModel responseModel =
            objectMapper.readValue(response.getContentAsString(), DeleteResponseModel.class);
        assertTrue("Valid delete response object state enumeration",
            (responseModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.DELETED ||
                responseModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.NOT_FOUND));
    }

}
