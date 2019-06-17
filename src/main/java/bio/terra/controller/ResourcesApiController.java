package bio.terra.controller;

import bio.terra.metadata.BillingProfile;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateBillingProfileModel;
import bio.terra.service.ProfileService;
import bio.terra.validation.BillingProfileRequestValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.RequestBody;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import java.util.Optional;
import java.util.UUID;

@Controller
public class ResourcesApiController implements ResourcesApi {

    private final ObjectMapper objectMapper;
    private final HttpServletRequest request;
    private final ProfileService profileService;
    private final BillingProfileRequestValidator billingProfileRequestValidator;

    @Autowired
    public ResourcesApiController(
            ObjectMapper objectMapper,
            HttpServletRequest request,
            ProfileService profileService,
            BillingProfileRequestValidator billingProfileRequestValidator) {
        this.objectMapper = objectMapper;
        this.request = request;
        this.profileService = profileService;
        this.billingProfileRequestValidator = billingProfileRequestValidator;
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
        binder.addValidators(billingProfileRequestValidator);
    }

    @Override
    public ResponseEntity<BillingProfileModel> createProfile(
        @RequestBody BillingProfileRequestModel billingProfileRequest) {
        return new ResponseEntity<>(profileService.createProfile(billingProfileRequest), HttpStatus.CREATED);
    }

    @Override
    public ResponseEntity<DeleteResponseModel> deleteProfile(String id) {
        UUID profileId = UUID.fromString(id);
        return new ResponseEntity<>(profileService.deleteProfileById(profileId), HttpStatus.OK);
    }

    @Override
    public ResponseEntity<EnumerateBillingProfileModel> enumerateProfiles(
            @Valid Integer offset,
            @Valid Integer limit) {
        ControllerUtils.validateEnumerateParams(offset, limit);
        EnumerateBillingProfileModel ebpm = profileService.enumerateProfiles(offset, limit);
        return new ResponseEntity<>(ebpm, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<BillingProfileModel> retrieveProfile(String id) {
        UUID profileId = UUID.fromString(id);
        BillingProfile profile = profileService.getProfileById(profileId);
        BillingProfileModel profileModel = profileService.makeModelFromBillingProfile(profile);
        return new ResponseEntity<>(profileModel, HttpStatus.OK);
    }
}
