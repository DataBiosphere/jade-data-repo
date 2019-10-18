package bio.terra.app.controller;

import bio.terra.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.service.resourcemanagement.ProfileDao;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import bio.terra.service.iam.SamClientService;
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

import java.util.UUID;

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
public class DatasetConnectedTest {
    @Autowired private MockMvc mvc;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private JsonLoader jsonLoader;
    @Autowired private ProfileDao profileDao;
    @Autowired private GoogleResourceConfiguration googleResourceConfiguration;
    @Autowired private ConnectedOperations connectedOperations;

    @MockBean private SamClientService samService;

    @Test
    public void testCreateOmopDataset() throws Exception {
        connectedOperations.stubOutSamCalls(samService);
        String accountId = googleResourceConfiguration.getCoreBillingAccount();

        DatasetRequestModel datasetRequest = jsonLoader.loadObject("it-dataset-omop.json", DatasetRequestModel.class);
        UUID profileId = profileDao.createBillingProfile(ProfileFixtures.billingProfileForAccount(accountId));
        datasetRequest
            .name(Names.randomizeName(datasetRequest.getName()))
            .defaultProfileId(profileId.toString());

        MvcResult result = mvc.perform(post("/api/repository/v1/datasets")
            .contentType(MediaType.APPLICATION_JSON)
            .content(objectMapper.writeValueAsString(datasetRequest)))
            .andExpect(status().isCreated())
            .andReturn();
        MockHttpServletResponse response = result.getResponse();
        assertThat("create omop dataset successfully", response.getStatus(), equalTo(HttpStatus.CREATED.value()));
        DatasetSummaryModel datasetSummaryModel =
            objectMapper.readValue(response.getContentAsString(), DatasetSummaryModel.class);

        result = mvc.perform(delete("/api/repository/v1/datasets/" + datasetSummaryModel.getId())).andReturn();
        response = result.getResponse();
        assertThat("delete omop dataset successfully", response.getStatus(), equalTo(HttpStatus.OK.value()));
        DeleteResponseModel responseModel =
            objectMapper.readValue(response.getContentAsString(), DeleteResponseModel.class);
        assertTrue("Valid delete response object state enumeration",
            (responseModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.DELETED ||
                responseModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.NOT_FOUND));
        profileDao.deleteBillingProfileById(profileId);
    }

}
