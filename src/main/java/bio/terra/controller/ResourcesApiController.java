package bio.terra.controller;

import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.service.ResourceService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import javax.servlet.http.HttpServletRequest;
import java.util.Optional;

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
    public ResponseEntity<BillingProfileModel> createProfile(
        @RequestBody BillingProfileRequestModel billingProfileRequest) { // TODO add validation and @Valid
        return new ResponseEntity<>(resourceService.createProfile(billingProfileRequest), HttpStatus.CREATED);
    }

    @Override
    public Optional<HttpServletRequest> getRequest() {
        return Optional.ofNullable(request);
    }
}
