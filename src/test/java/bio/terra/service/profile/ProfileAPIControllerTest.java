package bio.terra.service.profile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
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
import java.util.UUID;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletRequest;
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
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
public class ProfileAPIControllerTest {

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
    when(authenticatedUserRequestFactory.from(eq(request))).thenReturn(user);
    var billingProfileRequestModel = new BillingProfileRequestModel();
    String jobId = "jobId";
    when(profileService.createProfile(eq(billingProfileRequestModel), eq(user)))
        .thenReturn("jobId");

    var jobModel = new JobModel();
    jobModel.setJobStatus(JobStatusEnum.RUNNING);
    when(jobService.retrieveJob(eq(jobId), eq(user))).thenReturn(jobModel);

    ResponseEntity entity = apiController.createProfile(billingProfileRequestModel);
    verify(profileService, times(1)).createProfile(eq(billingProfileRequestModel), eq(user));
    assertNotNull(entity);
  }

  @Test
  void testUpdateProfile() {
    when(authenticatedUserRequestFactory.from(eq(request))).thenReturn(user);
    var billingProfileUpdateModel = new BillingProfileUpdateModel().id(UUID.randomUUID());
    String jobId = "jobId";
    when(profileService.updateProfile(eq(billingProfileUpdateModel), eq(user))).thenReturn(jobId);

    var jobModel = new JobModel();
    jobModel.setJobStatus(JobStatusEnum.RUNNING);
    when(jobService.retrieveJob(eq(jobId), eq(user))).thenReturn(jobModel);

    ResponseEntity entity = apiController.updateProfile(billingProfileUpdateModel);
    verify(profileService, times(1)).updateProfile(eq(billingProfileUpdateModel), eq(user));
    assertNotNull(entity);
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
    verify(profileService, never()).updateProfile(eq(billingProfileUpdateModel), eq(user));
  }

  @Test
  void testUpdateProfileForbidden() {
    when(authenticatedUserRequestFactory.from(eq(request))).thenReturn(user);
    UUID profileId = UUID.randomUUID();
    when(profileService.getProfileByIdNoCheck(profileId))
        .thenReturn(new BillingProfileModel().id(profileId));
    mockProfileForbidden(profileId, IamAction.UPDATE_BILLING_ACCOUNT);
    var billingProfileUpdateModel = new BillingProfileUpdateModel().id(profileId);
    assertThrows(
        IamForbiddenException.class, () -> apiController.updateProfile(billingProfileUpdateModel));
    verify(profileService, never()).updateProfile(eq(billingProfileUpdateModel), eq(user));
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
    when(profileService.deleteProfile(eq(deleteId), eq(deleteCloudResources), eq(user)))
        .thenReturn(jobId);
    doNothing().when(iamService).verifyAuthorization(any(), any(), any(), any());

    var jobModel = new JobModel();
    jobModel.setJobStatus(JobStatusEnum.RUNNING);
    when(jobService.retrieveJob(eq(jobId), eq(user))).thenReturn(jobModel);

    ResponseEntity entity = apiController.deleteProfile(deleteId, deleteCloudResources);
    // Only check for admin auth if deleteCloudResources is true
    verify(iamService, times(expectedAdminAuthNumberOfInvocations))
        .verifyAuthorization(
            eq(user), eq(IamResourceType.DATAREPO), any(), eq(IamAction.CONFIGURE));
    // Only check if user has access on the spend profile if we're not doing the admin check
    verify(iamService, times(expectedSpendProfileAuthNumberOfInvocations))
        .verifyAuthorization(
            eq(user),
            eq(IamResourceType.SPEND_PROFILE),
            eq(deleteId.toString()),
            eq(IamAction.DELETE));
    verify(profileService, times(1))
        .deleteProfile(eq(deleteId), eq(deleteCloudResources), eq(user));
    assertNotNull(entity);
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
    verify(profileService, never()).deleteProfile(eq(profileId), eq(false), eq(user));
  }

  @Test
  void testDeleteProfileForbidden() {
    when(authenticatedUserRequestFactory.from(eq(request))).thenReturn(user);
    UUID profileId = UUID.randomUUID();
    when(profileService.getProfileByIdNoCheck(profileId))
        .thenReturn(new BillingProfileModel().id(profileId));
    mockProfileForbidden(profileId, IamAction.DELETE);
    assertThrows(IamForbiddenException.class, () -> apiController.deleteProfile(profileId, false));
    verify(profileService, never()).deleteProfile(eq(profileId), eq(false), eq(user));
  }

  @Test
  void testAddProfilePolicyMember() {
    when(authenticatedUserRequestFactory.from(any())).thenReturn(user);

    UUID id = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    String policyName = "policyName";
    var policyMemberRequest = new PolicyMemberRequest();
    var policyModel = new PolicyModel();
    when(profileService.addProfilePolicyMember(
            eq(id), eq(policyName), eq(policyMemberRequest), eq(user)))
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
