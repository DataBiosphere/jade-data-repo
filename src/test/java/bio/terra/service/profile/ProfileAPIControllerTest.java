package bio.terra.service.profile;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.linkTo;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.methodOn;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.controller.GlobalExceptionHandler;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.BillingProfileUpdateModel;
import bio.terra.model.EnumerateBillingProfileResourcesModel;
import bio.terra.model.JobModel;
import bio.terra.model.JobModel.JobStatusEnum;
import bio.terra.model.PolicyMemberRequest;
import bio.terra.model.PolicyModel;
import bio.terra.model.PolicyResponse;
import bio.terra.model.ProfileOwnedResourceModel;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.PolicyMemberValidator;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import bio.terra.service.job.JobService;
import bio.terra.service.profile.exception.ProfileNotFoundException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
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
@Tag(Unit.TAG)
@WebMvcTest
class ProfileAPIControllerTest {
  @MockitoBean private ProfileService profileService;
  @MockitoBean private AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  @MockitoBean private JobService jobService;

  @MockitoBean private IamService iamService;
  @MockitoBean private ApplicationConfiguration applicationConfiguration;

  @Autowired private ObjectMapper objectMapper;
  @Autowired private MockMvc mvc;
  @Autowired ProfileApiController apiController;
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  private static <T> URI createUri(ResponseEntity<T> object) {
    return linkTo(object).toUri();
  }

  private static ProfileApiController getApi() {
    return methodOn(ProfileApiController.class);
  }

  @BeforeEach
  void setup() {
    when(authenticatedUserRequestFactory.from(any())).thenReturn(TEST_USER);
  }

  @Test
  void testCreateProfile() {
    var billingProfileRequestModel =
        new BillingProfileRequestModel().profileName("profileName").biller("biller");
    String jobId = "jobId";
    when(profileService.createProfile(billingProfileRequestModel, TEST_USER)).thenReturn(jobId);

    var jobModel = new JobModel();
    jobModel.setJobStatus(JobStatusEnum.RUNNING);
    when(jobService.retrieveJob(jobId, TEST_USER)).thenReturn(jobModel);

    ResponseEntity<JobModel> entity = apiController.createProfile(billingProfileRequestModel);
    assertThat("Correct job model is returned from request", entity.getBody(), is(jobModel));
  }

  private static BillingProfileUpdateModel createUpdateModel() {
    return new BillingProfileUpdateModel()
        .id(UUID.randomUUID())
        .billingAccountId("id")
        .description("description");
  }

  @Test
  void testUpdateProfile() {
    var billingProfileUpdateModel = createUpdateModel();
    String jobId = "jobId";
    when(profileService.updateProfile(billingProfileUpdateModel, TEST_USER)).thenReturn(jobId);

    var jobModel = new JobModel();
    jobModel.setJobStatus(JobStatusEnum.RUNNING);
    when(jobService.retrieveJob(jobId, TEST_USER)).thenReturn(jobModel);

    ResponseEntity<JobModel> entity = apiController.updateProfile(billingProfileUpdateModel);
    assertThat("Correct job model is returned from request", entity.getBody(), is(jobModel));
  }

  @Test
  void testUpdateProfileNotFound() {
    var billingProfileUpdateModel = createUpdateModel();
    doThrow(ProfileNotFoundException.class)
        .when(profileService)
        .getProfileByIdNoCheck(billingProfileUpdateModel.getId());
    assertThrows(
        ProfileNotFoundException.class,
        () -> apiController.updateProfile(billingProfileUpdateModel));
    verifyNoInteractions(iamService);
    verify(profileService, never()).updateProfile(billingProfileUpdateModel, TEST_USER);
  }

  @Test
  void testUpdateProfileForbidden() {
    var billingProfileUpdateModel = createUpdateModel();
    UUID profileId = billingProfileUpdateModel.getId();
    when(profileService.getProfileByIdNoCheck(profileId))
        .thenReturn(new BillingProfileModel().id(profileId));
    mockProfileForbidden(profileId, IamAction.UPDATE_BILLING_ACCOUNT);
    assertThrows(
        IamForbiddenException.class, () -> apiController.updateProfile(billingProfileUpdateModel));
    verify(profileService, never()).updateProfile(billingProfileUpdateModel, TEST_USER);
  }

  @ParameterizedTest
  @MethodSource
  void testDeleteProfile(
      boolean deleteCloudResources,
      int expectedAdminAuthNumberOfInvocations,
      int expectedSpendProfileAuthNumberOfInvocations) {
    UUID deleteId = UUID.randomUUID();
    String jobId = "jobId";
    when(profileService.deleteProfile(deleteId, deleteCloudResources, TEST_USER)).thenReturn(jobId);
    var applicationId = "broad-jade-dev";
    if (deleteCloudResources) {
      when(applicationConfiguration.getResourceId()).thenReturn(applicationId);
    }

    var jobModel = new JobModel();
    jobModel.setJobStatus(JobStatusEnum.RUNNING);
    when(jobService.retrieveJob(jobId, TEST_USER)).thenReturn(jobModel);

    ResponseEntity<JobModel> entity = apiController.deleteProfile(deleteId, deleteCloudResources);
    // Only check for admin auth if deleteCloudResources is true
    verify(iamService, times(expectedAdminAuthNumberOfInvocations))
        .verifyAuthorization(TEST_USER, IamResourceType.DATAREPO, applicationId, IamAction.DELETE);
    // Only check if user has access on the spend profile if we're not doing the admin check
    verify(iamService, times(expectedSpendProfileAuthNumberOfInvocations))
        .verifyAuthorization(
            TEST_USER, IamResourceType.SPEND_PROFILE, deleteId.toString(), IamAction.DELETE);
    assertThat("Correct job model is returned from delete request", entity.getBody(), is(jobModel));
  }

  private static Stream<Arguments> testDeleteProfile() {
    return Stream.of(arguments(true, 1, 0), arguments(false, 0, 1));
  }

  @Test
  void testDeleteProfileNotFound() {
    UUID profileId = UUID.randomUUID();
    doThrow(ProfileNotFoundException.class).when(profileService).getProfileByIdNoCheck(profileId);
    assertThrows(
        ProfileNotFoundException.class, () -> apiController.deleteProfile(profileId, false));
    verifyNoInteractions(iamService);
    verify(profileService, never()).deleteProfile(profileId, false, TEST_USER);
  }

  @Test
  void testDeleteProfileForbidden() {
    UUID profileId = UUID.randomUUID();
    when(profileService.getProfileByIdNoCheck(profileId))
        .thenReturn(new BillingProfileModel().id(profileId));
    mockProfileForbidden(profileId, IamAction.DELETE);
    assertThrows(IamForbiddenException.class, () -> apiController.deleteProfile(profileId, false));
    verify(profileService, never()).deleteProfile(profileId, false, TEST_USER);
  }

  @Test
  void testAddProfilePolicyMember() {
    UUID id = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    String policyName = "policyName";
    var policyMemberRequest = new PolicyMemberRequest().email("email");
    var policyModel = new PolicyModel();
    when(profileService.addProfilePolicyMember(id, policyName, policyMemberRequest, TEST_USER))
        .thenReturn(policyModel);

    ResponseEntity<PolicyResponse> response =
        apiController.addProfilePolicyMember(id, policyName, policyMemberRequest);

    assertTrue(response.getBody().getPolicies().contains(policyModel));
    assertEquals(HttpStatus.OK, response.getStatusCode());
  }

  private void mockProfileForbidden(UUID profileId, IamAction action) {
    doThrow(IamForbiddenException.class)
        .when(iamService)
        .verifyAuthorization(
            TEST_USER, IamResourceType.SPEND_PROFILE, profileId.toString(), action);
  }

  @Test
  void getProfileResources() throws Exception {
    var dataset =
        new ProfileOwnedResource(
            UUID.randomUUID(),
            "name",
            "description",
            Instant.now(),
            ProfileOwnedResource.Type.DATASET);
    var snapshot =
        new ProfileOwnedResource(
            UUID.randomUUID(),
            "name",
            "description",
            Instant.now(),
            ProfileOwnedResource.Type.SNAPSHOT);
    var model =
        new EnumerateBillingProfileResourcesModel()
            .items(
                List.of(
                    new ProfileOwnedResourceModel()
                        .id(dataset.id())
                        .name(dataset.name())
                        .description(dataset.description())
                        .type(ProfileOwnedResourceModel.TypeEnum.DATASET)
                        .createdDate(dataset.createdDate().toString()),
                    new ProfileOwnedResourceModel()
                        .id(snapshot.id())
                        .name(snapshot.name())
                        .description(snapshot.description())
                        .type(ProfileOwnedResourceModel.TypeEnum.SNAPSHOT)
                        .createdDate(snapshot.createdDate().toString())));
    UUID profileId = UUID.randomUUID();
    when(profileService.getProfileResources(profileId)).thenReturn(List.of(dataset, snapshot));
    mvc.perform(get(createUri(getApi().getProfileResources(profileId))))
        .andExpect(status().isOk())
        .andExpect(content().json(objectMapper.writeValueAsString(model)));
  }

  @Test
  void getProfileResourcesForbidden() throws Exception {
    UUID profileId = UUID.randomUUID();
    mockProfileForbidden(profileId, IamAction.LIST_CHILDREN);
    mvc.perform(get(createUri(getApi().getProfileResources(profileId))))
        .andExpect(status().isForbidden());
  }
}
