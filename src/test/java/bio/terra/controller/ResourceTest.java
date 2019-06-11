package bio.terra.controller;

import bio.terra.category.Unit;
import bio.terra.controller.exception.ApiException;
import bio.terra.dao.ResourceDao;
import bio.terra.dao.StudyDao;
import bio.terra.dao.exception.StudyNotFoundException;
import bio.terra.fixtures.FlightStates;
import bio.terra.fixtures.JsonLoader;
import bio.terra.fixtures.ResourceFixtures;
import bio.terra.flight.study.create.StudyCreateFlight;
import bio.terra.metadata.BillingProfile;
import bio.terra.metadata.Study;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.StudyJsonConversion;
import bio.terra.model.StudyModel;
import bio.terra.model.StudyRequestModel;
import bio.terra.service.SamClientService;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.Stairway;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
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
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.UUID;

import static bio.terra.fixtures.StudyFixtures.buildStudyRequest;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class ResourceTest {

    @Autowired
    private MockMvc mvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JsonLoader jsonLoader;

    private BillingProfileRequestModel billingProfileRequest;


    @Before
    public void setup() throws Exception {
        billingProfileRequest = requestModel("billing-profile.json");
    }

    private BillingProfileRequestModel requestModel(String jsonResourceFileName) throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        String studyJsonStr = IOUtils.toString(classLoader.getResourceAsStream(jsonResourceFileName));
        return objectMapper.readerFor(BillingProfileRequestModel.class).readValue(studyJsonStr);
    }

    @Test
    public void testCreateReadDelete() throws Exception {
        String accountId = ResourceFixtures.randomBillingAccountId();
        billingProfileRequest.billingAccountId(accountId);
        String responseJson = mvc.perform(post("/api/resources/v1/profiles")
            .contentType(MediaType.APPLICATION_JSON)
            .header("Authorization", "Bearer: faketoken")
            .content(objectMapper.writeValueAsString(billingProfileRequest)))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.profileName").value("Test billing account"))
            .andExpect(jsonPath("$.biller").value("direct"))
            .andExpect(jsonPath("$.billingAccountId").value(accountId))
            .andReturn().getResponse().getContentAsString();
        BillingProfileModel profileModel = objectMapper.readerFor(BillingProfileModel.class).readValue(responseJson);
        String profileId = profileModel.getId();

        mvc.perform(get("/api/resources/v1/profiles/" + profileId)
            .contentType(MediaType.APPLICATION_JSON)
            .header("Authorization", "Bearer: faketoken"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.profileName").value("Test billing account"))
            .andExpect(jsonPath("$.biller").value("direct"))
            .andExpect(jsonPath("$.billingAccountId").value(accountId))
            .andExpect(jsonPath("$.id").value(profileId));

        mvc.perform(delete("/api/resources/v1/profiles/" + profileId)
            .contentType(MediaType.APPLICATION_JSON)
            .header("Authorization", "Bearer: faketoken"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.objectState").value("deleted"));

        mvc.perform(delete("/api/resources/v1/profiles/" + profileId)
            .contentType(MediaType.APPLICATION_JSON)
            .header("Authorization", "Bearer: faketoken"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.objectState").value("not_found"));

        mvc.perform(get("/api/resources/v1/profiles/" + profileId)
            .contentType(MediaType.APPLICATION_JSON)
            .header("Authorization", "Bearer: faketoken"))
            .andExpect(status().isNotFound());
    }

    @Test
    public void testProfileRetrieve() throws Exception {
        assertThat("Profile retrieve with bad id gets 400",
                mvc.perform(get("/api/resources/v1/profiles/{id}", "blah"))
                        .andReturn().getResponse().getStatus(),
                equalTo(HttpStatus.BAD_REQUEST.value()));
    }

    @Test
    public void testBadAccount() throws Exception {
        String accountId = "blah";
        billingProfileRequest.billingAccountId(accountId);
        String responseJson = mvc.perform(post("/api/resources/v1/profiles")
            .contentType(MediaType.APPLICATION_JSON)
            .header("Authorization", "Bearer: faketoken")
            .content(objectMapper.writeValueAsString(billingProfileRequest)))
            .andExpect(status().is4xxClientError())
            .andReturn().getResponse().getContentAsString();
        ErrorModel errors = objectMapper.readerFor(ErrorModel.class).readValue(responseJson);
        assertThat("invalid billing account", errors.getErrorDetail().get(0), startsWith("billingAccountId"));
    }


}
