package bio.terra.app.controller;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.JobModel;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.PolicyMemberValidator;
import bio.terra.service.job.JobService;
import bio.terra.service.profile.ProfileApiController;
import bio.terra.service.profile.ProfileRequestValidator;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.ProfileUpdateRequestValidator;
import bio.terra.service.profile.exception.ProfileNotFoundException;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@ActiveProfiles({"google", "unittest"})
@ContextConfiguration(
    classes = {
      ProfileApiController.class,
      ProfileRequestValidator.class,
      ProfileUpdateRequestValidator.class,
      PolicyMemberValidator.class,
      GlobalExceptionHandler.class
    })
@MockitoBean(types = {ApplicationConfiguration.class})
@Tag(Unit.TAG)
@WebMvcTest
class ProfileTest {

  @Autowired private MockMvc mvc;
  @MockitoBean private ProfileService profileService;
  @MockitoBean private JobService jobService;
  @MockitoBean private AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  @MockitoBean private IamService iamService;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  private final BillingProfileRequestModel billingProfileRequest =
      ProfileFixtures.randomBillingProfileRequest();

  @BeforeEach
  void beforeEach() {
    when(authenticatedUserRequestFactory.from(any())).thenReturn(TEST_USER);
  }

  @Test
  void createProfile() throws Exception {
    String jobId = "job id";
    when(profileService.createProfile(billingProfileRequest, TEST_USER)).thenReturn(jobId);
    JobModel expected = mockRetrieveJob(jobId);
    String responseJson =
        mvc.perform(
                post("/api/resources/v1/profiles")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(billingProfileRequest)))
            .andExpect(status().isAccepted())
            .andReturn()
            .getResponse()
            .getContentAsString();
    JobModel jobModel = TestUtils.mapFromJson(responseJson, JobModel.class);
    assertThat(jobModel, is(expected));
    verifyNoInteractions(iamService);
  }

  private JobModel mockRetrieveJob(String jobId) {
    JobModel expected =
        new JobModel()
            .jobStatus(JobModel.JobStatusEnum.RUNNING)
            .id(jobId)
            .description("description");
    when(jobService.retrieveJob(jobId, TEST_USER)).thenReturn(expected);
    return expected;
  }

  @Test
  void retrieveProfile() throws Exception {
    UUID profileId = UUID.randomUUID();
    BillingProfileModel expected = new BillingProfileModel().profileName("test profile");
    when(profileService.getProfileById(profileId, TEST_USER)).thenReturn(expected);
    String responseJson =
        mvc.perform(
                get("/api/resources/v1/profiles/{id}", profileId)
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();
    BillingProfileModel billingProfileModel =
        TestUtils.mapFromJson(responseJson, BillingProfileModel.class);
    assertThat(billingProfileModel, is(expected));
    verifyNoInteractions(iamService);
  }

  @Test
  void deleteProfile() throws Exception {
    UUID profileId = UUID.randomUUID();
    String jobId = "job id";
    when(profileService.deleteProfile(profileId, false, TEST_USER)).thenReturn(jobId);
    JobModel expected = mockRetrieveJob(jobId);
    String responseJson =
        mvc.perform(
                delete("/api/resources/v1/profiles/{id}", profileId)
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isAccepted())
            .andReturn()
            .getResponse()
            .getContentAsString();
    JobModel jobModel = TestUtils.mapFromJson(responseJson, JobModel.class);
    assertThat(jobModel, is(expected));
    verify(profileService).getProfileByIdNoCheck(profileId);
    verify(iamService)
        .verifyAuthorization(
            TEST_USER, IamResourceType.SPEND_PROFILE, profileId.toString(), IamAction.DELETE);
  }

  @Test
  void deleteProfileCloudResources() throws Exception {
    UUID profileId = UUID.randomUUID();
    String jobId = "job id";
    when(profileService.deleteProfile(profileId, true, TEST_USER)).thenReturn(jobId);
    JobModel expected = mockRetrieveJob(jobId);
    String responseJson =
        mvc.perform(
                delete("/api/resources/v1/profiles/{id}?deleteCloudResources=true", profileId)
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isAccepted())
            .andReturn()
            .getResponse()
            .getContentAsString();
    JobModel jobModel = TestUtils.mapFromJson(responseJson, JobModel.class);
    assertThat(jobModel, is(expected));
    verify(profileService).getProfileByIdNoCheck(profileId);
    verify(iamService)
        .verifyAuthorization(TEST_USER, IamResourceType.DATAREPO, null, IamAction.DELETE);
  }

  @Test
  void testGetNotFound() throws Exception {
    UUID profileId = UUID.randomUUID();
    when(profileService.getProfileById(profileId, TEST_USER))
        .thenThrow(new ProfileNotFoundException(""));
    mvc.perform(
            get("/api/resources/v1/profiles/{id}", profileId)
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  @Test
  void testProfileRetrieve() throws Exception {
    mvc.perform(get("/api/resources/v1/profiles/{id}", "blah")).andExpect(status().isBadRequest());
  }

  @Test
  void testBadAccount() throws Exception {
    billingProfileRequest.billingAccountId("blah");
    String responseJson =
        mvc.perform(
                post("/api/resources/v1/profiles")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(billingProfileRequest)))
            .andExpect(status().is4xxClientError())
            .andReturn()
            .getResponse()
            .getContentAsString();
    ErrorModel errors = TestUtils.mapFromJson(responseJson, ErrorModel.class);
    assertThat("invalid billing account", errors.getMessage(), containsString("billingAccountId"));
  }
}
