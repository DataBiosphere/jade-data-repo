package bio.terra.app.controller;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.ErrorModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)

// This test no longer works, because the API performs authorization checks.
// TODO: Replace this with a connected or integration test (DR-1460)
@Ignore
public class ProfileTest {

  @Autowired private MockMvc mvc;

  @Autowired private ObjectMapper objectMapper;

  @Autowired private JsonLoader jsonLoader;

  @Test
  public void testCreateReadDelete() throws Exception {
    BillingProfileRequestModel billingProfileRequest =
        ProfileFixtures.randomBillingProfileRequest();
    String accountId = billingProfileRequest.getBillingAccountId();
    String responseJson =
        mvc.perform(
                post("/api/resources/v1/profiles")
                    .contentType(MediaType.APPLICATION_JSON)
                    .header("Authorization", "Bearer: faketoken")
                    .content(TestUtils.mapToJson(billingProfileRequest)))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.profileName").value("Test billing account"))
            .andExpect(jsonPath("$.biller").value("direct"))
            .andExpect(jsonPath("$.billingAccountId").value(accountId))
            .andReturn()
            .getResponse()
            .getContentAsString();
    BillingProfileModel profileModel =
        objectMapper.readerFor(BillingProfileModel.class).readValue(responseJson);
    UUID profileId = profileModel.getId();

    mvc.perform(
            get("/api/resources/v1/profiles/" + profileId)
                .contentType(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer: faketoken"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.profileName").value("Test billing account"))
        .andExpect(jsonPath("$.biller").value("direct"))
        .andExpect(jsonPath("$.billingAccountId").value(accountId))
        .andExpect(jsonPath("$.id").value(profileId));

    mvc.perform(
            delete("/api/resources/v1/profiles/" + profileId)
                .contentType(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer: faketoken"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.objectState").value("deleted"));

    mvc.perform(
            delete("/api/resources/v1/profiles/" + profileId)
                .contentType(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer: faketoken"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.objectState").value("not_found"));

    mvc.perform(
            get("/api/resources/v1/profiles/" + profileId)
                .contentType(MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer: faketoken"))
        .andExpect(status().isNotFound());
  }

  @Test
  public void testProfileRetrieve() throws Exception {
    assertThat(
        "Profile retrieve with bad id gets 400",
        mvc.perform(get("/api/resources/v1/profiles/{id}", "blah"))
            .andReturn()
            .getResponse()
            .getStatus(),
        equalTo(HttpStatus.BAD_REQUEST.value()));
  }

  @Test
  public void testBadAccount() throws Exception {
    BillingProfileRequestModel billingProfileRequest =
        ProfileFixtures.randomBillingProfileRequest();
    billingProfileRequest.billingAccountId("blah");
    String responseJson =
        mvc.perform(
                post("/api/resources/v1/profiles")
                    .contentType(MediaType.APPLICATION_JSON)
                    .header("Authorization", "Bearer: faketoken")
                    .content(TestUtils.mapToJson(billingProfileRequest)))
            .andExpect(status().is4xxClientError())
            .andReturn()
            .getResponse()
            .getContentAsString();
    ErrorModel errors = objectMapper.readerFor(ErrorModel.class).readValue(responseJson);
    assertThat(
        "invalid billing account", errors.getErrorDetail().get(0), startsWith("billingAccountId"));
  }
}
