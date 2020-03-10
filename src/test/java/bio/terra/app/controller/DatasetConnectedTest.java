package bio.terra.app.controller;

import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.ErrorModel;
import bio.terra.service.iam.IamService;
import bio.terra.service.resourcemanagement.BillingProfile;
import bio.terra.service.resourcemanagement.ProfileDao;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
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
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultMatcher;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
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

    @MockBean private IamService samService;

    private BillingProfile billingProfile;

    @Before
    public void setup() throws Exception {
        connectedOperations.stubOutSamCalls(samService);
        billingProfile = ProfileFixtures.billingProfileForAccount(googleResourceConfiguration.getCoreBillingAccount());
        UUID profileId = profileDao.createBillingProfile(billingProfile);
        billingProfile.id(profileId);
    }

    @After
    public void tearDown() throws Exception {
        profileDao.deleteBillingProfileById(billingProfile.getId());
    }

    @Test
    public void testCreateOmopDataset() throws Exception {
        DatasetRequestModel datasetRequest = jsonLoader.loadObject("it-dataset-omop.json", DatasetRequestModel.class);
        datasetRequest
            .name(Names.randomizeName(datasetRequest.getName()))
            .defaultProfileId(billingProfile.getId().toString());

        MockHttpServletResponse response = createDataset(datasetRequest, status().isCreated());
        assertThat("create omop dataset successfully", response.getStatus(), equalTo(HttpStatus.CREATED.value()));
        DatasetSummaryModel datasetSummaryModel =
            objectMapper.readValue(response.getContentAsString(), DatasetSummaryModel.class);

        response = deleteDataset(datasetSummaryModel.getId());
        checkDeleteSuccessful(response);
    }

    @Test
    public void testDuplicateName() throws Exception {
        // create a dataset and check that it succeeds
        DatasetRequestModel datasetRequest = jsonLoader
            .loadObject("snapshot-test-dataset.json", DatasetRequestModel.class);
        datasetRequest
            .name(Names.randomizeName(datasetRequest.getName()))
            .defaultProfileId(billingProfile.getId().toString());

        MockHttpServletResponse response = createDataset(datasetRequest, status().isCreated());
        assertThat("created test dataset successfully", response.getStatus(), equalTo(HttpStatus.CREATED.value()));
        DatasetSummaryModel datasetSummaryModel =
            objectMapper.readValue(response.getContentAsString(), DatasetSummaryModel.class);

        // try to create the same dataset again and check that it fails
        response = createDataset(datasetRequest, status().is4xxClientError());
        assertThat("duplicate create dataset failed", response.getStatus(),
            equalTo(HttpStatus.BAD_REQUEST.value()));
        ErrorModel errorModel = checkResponseIsError(response);
        assertThat(errorModel.getMessage(),
            containsString("duplicate key value violates unique constraint \"dataset_name_key\""));

        // delete the dataset
        response = deleteDataset(datasetSummaryModel.getId());
        checkDeleteSuccessful(response);
    }

    private MockHttpServletResponse createDataset(DatasetRequestModel datasetRequest, ResultMatcher expectedStatus)
        throws Exception {
        MvcResult result = mvc.perform(post("/api/repository/v1/datasets")
            .contentType(MediaType.APPLICATION_JSON)
            .content(objectMapper.writeValueAsString(datasetRequest)))
            .andExpect(expectedStatus)
            .andReturn();
        return result.getResponse();
    }

    private MockHttpServletResponse deleteDataset(String datasetId) throws Exception {
        MvcResult result = mvc.perform(delete("/api/repository/v1/datasets/" + datasetId)).andReturn();
        return result.getResponse();
    }

    private void checkDeleteSuccessful(MockHttpServletResponse deleteResponse) throws Exception {
        assertThat("deleted dataset successfully", deleteResponse.getStatus(), equalTo(HttpStatus.OK.value()));
        DeleteResponseModel responseModel =
            objectMapper.readValue(deleteResponse.getContentAsString(), DeleteResponseModel.class);
        assertTrue("Valid delete response object state enumeration",
            (responseModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.DELETED ||
                responseModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.NOT_FOUND));
    }

    private ErrorModel checkResponseIsError(MockHttpServletResponse response) throws Exception {
        String responseBody = response.getContentAsString();
        HttpStatus responseStatus = HttpStatus.valueOf(response.getStatus());
        assertFalse("Expect HTTP failure status", responseStatus.is2xxSuccessful());

        assertTrue("Error model was returned on failure",
            StringUtils.contains(responseBody, "message"));

        return objectMapper.readValue(responseBody, ErrorModel.class);
    }
}
