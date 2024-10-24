package bio.terra.service.profile;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.BillingProfileUpdateModel;
import bio.terra.model.JobModel;
import bio.terra.model.JobModel.JobStatusEnum;
import bio.terra.model.PolicyMemberRequest;
import bio.terra.model.PolicyModel;
import bio.terra.model.PolicyResponse;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.PolicyMemberValidator;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import bio.terra.service.job.JobService;
import bio.terra.service.profile.exception.ProfileNotFoundException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class ProfileAPIControllerTest {

  @Mock private ObjectMapper objectMapper;
  @Mock private HttpServletRequest request;
  @Mock private ProfileService profileService;
  @Mock private ProfileRequestValidator billingProfileRequestValidator;
  @Mock private ProfileUpdateRequestValidator profileUpdateRequestValidator;
  @Mock private PolicyMemberValidator policyMemberValidator;
  @Mock private AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  @Mock private JobService jobService;

  @Mock private IamService iamService;
  @Mock private ApplicationConfiguration applicationConfiguration;

  private ProfileApiController apiController;
  private AuthenticatedUserRequest user;

  @BeforeEach
  void setup() {
    apiController =
        new ProfileApiController(
            objectMapper,
            request,
            profileService,
            billingProfileRequestValidator,
            profileUpdateRequestValidator,
            policyMemberValidator,
            jobService,
            authenticatedUserRequestFactory,
            iamService,
            applicationConfiguration);
    user =
        AuthenticatedUserRequest.builder()
            .setSubjectId("DatasetUnit")
            .setEmail("dataset@unit.com")
            .setToken("token")
            .build();
  }

  @Test
  void testCreateProfile() {
    when(authenticatedUserRequestFactory.from(request)).thenReturn(user);
    var billingProfileRequestModel = new BillingProfileRequestModel();
    String jobId = "jobId";
    when(profileService.createProfile(billingProfileRequestModel, user)).thenReturn("jobId");

    var jobModel = new JobModel();
    jobModel.setJobStatus(JobStatusEnum.RUNNING);
    when(jobService.retrieveJob(jobId, user)).thenReturn(jobModel);

    ResponseEntity<JobModel> entity = apiController.createProfile(billingProfileRequestModel);
    assertThat("Correct job model is returned from request", entity.getBody(), is(jobModel));
  }

  @Test
  void testUpdateProfile() {
    when(authenticatedUserRequestFactory.from(request)).thenReturn(user);
    var billingProfileUpdateModel = new BillingProfileUpdateModel().id(UUID.randomUUID());
    String jobId = "jobId";
    when(profileService.updateProfile(billingProfileUpdateModel, user)).thenReturn(jobId);

    var jobModel = new JobModel();
    jobModel.setJobStatus(JobStatusEnum.RUNNING);
    when(jobService.retrieveJob(jobId, user)).thenReturn(jobModel);

    ResponseEntity<JobModel> entity = apiController.updateProfile(billingProfileUpdateModel);
    assertThat("Correct job model is returned from request", entity.getBody(), is(jobModel));
  }

  @Test
  void testUpdateProfileNotFound() {
    UUID profileId = UUID.randomUUID();
    doThrow(ProfileNotFoundException.class).when(profileService).getProfileByIdNoCheck(profileId);
    var billingProfileUpdateModel = new BillingProfileUpdateModel().id(profileId);
    assertThrows(
        ProfileNotFoundException.class,
        () -> apiController.updateProfile(billingProfileUpdateModel));
    verifyNoInteractions(iamService);
    verify(profileService, never()).updateProfile(billingProfileUpdateModel, user);
  }

  @Test
  void testUpdateProfileForbidden() {
    when(authenticatedUserRequestFactory.from(request)).thenReturn(user);
    UUID profileId = UUID.randomUUID();
    when(profileService.getProfileByIdNoCheck(profileId))
        .thenReturn(new BillingProfileModel().id(profileId));
    mockProfileForbidden(profileId, IamAction.UPDATE_BILLING_ACCOUNT);
    var billingProfileUpdateModel = new BillingProfileUpdateModel().id(profileId);
    assertThrows(
        IamForbiddenException.class, () -> apiController.updateProfile(billingProfileUpdateModel));
    verify(profileService, never()).updateProfile(billingProfileUpdateModel, user);
  }

  @ParameterizedTest
  @MethodSource
  void testDeleteProfile(
      boolean deleteCloudResources,
      int expectedAdminAuthNumberOfInvocations,
      int expectedSpendProfileAuthNumberOfInvocations) {
    when(authenticatedUserRequestFactory.from(any())).thenReturn(user);
    UUID deleteId = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    String jobId = "jobId";
    when(profileService.deleteProfile(deleteId, deleteCloudResources, user)).thenReturn(jobId);
    var applicationId = "broad-jade-dev";
    if (deleteCloudResources) {
      when(applicationConfiguration.getResourceId()).thenReturn(applicationId);
    }

    var jobModel = new JobModel();
    jobModel.setJobStatus(JobStatusEnum.RUNNING);
    when(jobService.retrieveJob(jobId, user)).thenReturn(jobModel);

    ResponseEntity<JobModel> entity = apiController.deleteProfile(deleteId, deleteCloudResources);
    // Only check for admin auth if deleteCloudResources is true
    verify(iamService, times(expectedAdminAuthNumberOfInvocations))
        .verifyAuthorization(
            eq(user), eq(IamResourceType.DATAREPO), eq(applicationId), eq(IamAction.DELETE));
    // Only check if user has access on the spend profile if we're not doing the admin check
    verify(iamService, times(expectedSpendProfileAuthNumberOfInvocations))
        .verifyAuthorization(
            user, IamResourceType.SPEND_PROFILE, deleteId.toString(), IamAction.DELETE);
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
    verify(profileService, never()).deleteProfile(profileId, false, user);
  }

  @Test
  void testDeleteProfileForbidden() {
    when(authenticatedUserRequestFactory.from(request)).thenReturn(user);
    UUID profileId = UUID.randomUUID();
    when(profileService.getProfileByIdNoCheck(profileId))
        .thenReturn(new BillingProfileModel().id(profileId));
    mockProfileForbidden(profileId, IamAction.DELETE);
    assertThrows(IamForbiddenException.class, () -> apiController.deleteProfile(profileId, false));
    verify(profileService, never()).deleteProfile(profileId, false, user);
  }

  @Test
  void testAddProfilePolicyMember() {
    when(authenticatedUserRequestFactory.from(any())).thenReturn(user);

    UUID id = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    String policyName = "policyName";
    var policyMemberRequest = new PolicyMemberRequest();
    var policyModel = new PolicyModel();
    when(profileService.addProfilePolicyMember(id, policyName, policyMemberRequest, user))
        .thenReturn(policyModel);

    ResponseEntity<PolicyResponse> response =
        apiController.addProfilePolicyMember(id, policyName, policyMemberRequest);

    assertTrue(response.getBody().getPolicies().contains(policyModel));
    assertEquals(HttpStatus.OK, response.getStatusCode());
  }

  private void mockProfileForbidden(UUID profileId, IamAction action) {
    doThrow(IamForbiddenException.class)
        .when(iamService)
        .verifyAuthorization(user, IamResourceType.SPEND_PROFILE, profileId.toString(), action);
  }
}
