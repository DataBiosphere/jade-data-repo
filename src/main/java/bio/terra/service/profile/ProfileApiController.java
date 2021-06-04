package bio.terra.service.profile;

import bio.terra.app.utils.ControllerUtils;
import bio.terra.controller.ProfilesApi;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.BillingProfileUpdateModel;
import bio.terra.model.EnumerateBillingProfileModel;
import bio.terra.model.JobModel;
import bio.terra.model.PolicyMemberRequest;
import bio.terra.model.PolicyModel;
import bio.terra.model.PolicyResponse;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.AuthenticatedUserRequestFactory;
import bio.terra.service.iam.PolicyMemberValidator;
import bio.terra.service.job.JobService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static bio.terra.app.utils.ControllerUtils.jobToResponse;

@Controller
@Api(tags = {"profiles"})
public class ProfileApiController implements ProfilesApi {

    private final ObjectMapper objectMapper;
    private final HttpServletRequest request;
    private final ProfileService profileService;
    private final ProfileRequestValidator billingProfileRequestValidator;
    private final ProfileUpdateRequestValidator profileUpdateRequestValidator;
    private final PolicyMemberValidator policyMemberValidator;
    private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
    private final JobService jobService;

    @Autowired
    public ProfileApiController(
        ObjectMapper objectMapper,
        HttpServletRequest request,
        ProfileService profileService,
        ProfileRequestValidator billingProfileRequestValidator,
        ProfileUpdateRequestValidator profileUpdateRequestValidator,
        PolicyMemberValidator policyMemberValidator,
        JobService jobService,
        AuthenticatedUserRequestFactory authenticatedUserRequestFactory) {
        this.objectMapper = objectMapper;
        this.request = request;
        this.profileService = profileService;
        this.billingProfileRequestValidator = billingProfileRequestValidator;
        this.profileUpdateRequestValidator = profileUpdateRequestValidator;
        this.policyMemberValidator = policyMemberValidator;
        this.jobService = jobService;
        this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
    }

    @Override
    public Optional<ObjectMapper> getObjectMapper() {
        return Optional.ofNullable(objectMapper);
    }

    @Override
    public Optional<HttpServletRequest> getRequest() {
        return Optional.ofNullable(request);
    }

    @InitBinder
    protected void initBinder(final WebDataBinder binder) {
        binder.addValidators(profileUpdateRequestValidator);
        binder.addValidators(billingProfileRequestValidator);
        binder.addValidators(policyMemberValidator);
    }

    @Override
    public ResponseEntity<JobModel> createProfile(
        @RequestBody BillingProfileRequestModel billingProfileRequest) {
        AuthenticatedUserRequest user = authenticatedUserRequestFactory.from(request);
        String jobId = profileService.createProfile(billingProfileRequest, user);
        return jobToResponse(jobService.retrieveJob(jobId, user));
    }

    @Override
    public ResponseEntity<JobModel> updateProfile(
        @Valid @RequestBody  BillingProfileUpdateModel billingProfileRequest) {
        AuthenticatedUserRequest user = authenticatedUserRequestFactory.from(request);
        String jobId = profileService.updateProfile(billingProfileRequest, user);
        return jobToResponse(jobService.retrieveJob(jobId, user));
    }

    @Override
    public ResponseEntity<JobModel> deleteProfile(UUID id) {
        AuthenticatedUserRequest user = authenticatedUserRequestFactory.from(request);
        String jobId = profileService.deleteProfile(id, user);
        return jobToResponse(jobService.retrieveJob(jobId, user));
    }

    @Override
    public ResponseEntity<EnumerateBillingProfileModel> enumerateProfiles(
        @Valid @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
        @Valid @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) {
        ControllerUtils.validateEnumerateParams(offset, limit);
        AuthenticatedUserRequest user = authenticatedUserRequestFactory.from(request);
        EnumerateBillingProfileModel ebpm = profileService.enumerateProfiles(offset, limit, user);
        return new ResponseEntity<>(ebpm, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<BillingProfileModel> retrieveProfile(UUID id) {
        AuthenticatedUserRequest user = authenticatedUserRequestFactory.from(request);
        BillingProfileModel profileModel = profileService.getProfileById(id, user);
        return new ResponseEntity<>(profileModel, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<PolicyResponse> addProfilePolicyMember(
        @PathVariable("id") UUID id,
        @PathVariable("policyName") String policyName,
        @Valid @RequestBody PolicyMemberRequest policyMember) {
        AuthenticatedUserRequest user = authenticatedUserRequestFactory.from(request);
        PolicyModel policy = profileService.addProfilePolicyMember(id, policyName, policyMember, user);
        PolicyResponse response = new PolicyResponse().policies(Collections.singletonList(policy));
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<PolicyResponse> deleteProfilePolicyMember(
        @PathVariable("id") UUID id,
        @PathVariable("policyName") String policyName,
        @PathVariable("memberEmail") String memberEmail) {
        AuthenticatedUserRequest user = authenticatedUserRequestFactory.from(request);
        PolicyModel policy = profileService.deleteProfilePolicyMember(id, policyName, memberEmail, user);
        PolicyResponse response = new PolicyResponse().policies(Collections.singletonList(policy));
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<PolicyResponse> retrieveProfilePolicies(@PathVariable("id") UUID id) {
        AuthenticatedUserRequest user = authenticatedUserRequestFactory.from(request);
        List<PolicyModel> policies = profileService.retrieveProfilePolicies(id, user);
        PolicyResponse response = new PolicyResponse().policies(policies);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}
