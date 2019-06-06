package bio.terra.controller;

import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
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

    @Autowired
    public ResourcesApiController(ObjectMapper objectMapper, HttpServletRequest request) {
        this.objectMapper = objectMapper;
        this.request = request;
    }

    @Override
    public Optional<ObjectMapper> getObjectMapper() {
        return Optional.ofNullable(objectMapper);
    }

    @Override
    @RequestMapping(value = "/api/resources/v1/profiles",
        produces = { "application/json" },
        consumes = { "application/json" },
        method = RequestMethod.POST)
    public ResponseEntity<BillingProfileModel> createProfile(
        @RequestBody BillingProfileRequestModel billingProfileRequest) { // TODO add validation and @Valid
        return resourceService.createProfile(billingProfileRequest);
    }

    @Override
    public Optional<HttpServletRequest> getRequest() {
        return Optional.ofNullable(request);
    }
}
