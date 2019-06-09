package bio.terra.controller;

import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateBillingProfileModel;
import bio.terra.service.ResourceService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import java.util.Optional;
import java.util.UUID;

@Controller
public class ResourcesApiController implements ResourcesApi {

    private final ObjectMapper objectMapper;

    private final HttpServletRequest request;
    private ResourceService resourceService;

    @Autowired
    public ResourcesApiController(
            ObjectMapper objectMapper,
            HttpServletRequest request,
            ResourceService resourceService) {
        this.objectMapper = objectMapper;
        this.request = request;
        this.resourceService = resourceService;
    }

    @Override
    public Optional<ObjectMapper> getObjectMapper() {
        return Optional.ofNullable(objectMapper);
    }

    @Override
    public Optional<HttpServletRequest> getRequest() {
        return Optional.ofNullable(request);
    }

    @Override
    public ResponseEntity<BillingProfileModel> createProfile(
        @RequestBody BillingProfileRequestModel billingProfileRequest) { // TODO add validation and @Valid
        return new ResponseEntity<>(resourceService.createProfile(billingProfileRequest), HttpStatus.CREATED);
    }

    @Override
    public ResponseEntity<DeleteResponseModel> deleteProfile(String id) {
        return null;
    }

    @Override
    public ResponseEntity<EnumerateBillingProfileModel> enumerateProfiles(
            @Valid Integer offset,
            @Valid Integer limit) {
        ControllerUtils.validateEnumerateParams(offset, limit);
        EnumerateBillingProfileModel ebpm = resourceService.enumerateProfiles(offset, limit);
        return new ResponseEntity<>(ebpm, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<BillingProfileModel> retrieveProfile(String id) {
        UUID profileId = UUID.fromString(id);
        BillingProfileModel profileModel = resourceService.getProfileById(profileId);
        return new ResponseEntity<>(profileModel, HttpStatus.OK);
    }
}
