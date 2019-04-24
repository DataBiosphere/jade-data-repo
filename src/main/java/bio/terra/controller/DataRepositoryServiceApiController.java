package bio.terra.controller;

import bio.terra.configuration.ApplicationConfiguration;
import bio.terra.controller.exception.ValidationException;
import bio.terra.model.DRSAccessURL;
import bio.terra.model.DRSBundle;
import bio.terra.model.DRSObject;
import bio.terra.model.DRSServiceInfo;
import bio.terra.service.FileService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;

import javax.servlet.http.HttpServletRequest;
import java.util.Optional;

@Controller
public class DataRepositoryServiceApiController implements DataRepositoryServiceApi {

    private Logger logger = LoggerFactory.getLogger(DataRepositoryServiceApiController.class);

    private final ObjectMapper objectMapper;
    private final HttpServletRequest request;
    private final FileService fileService;

    // needed for local testing w/o proxy
    private final ApplicationConfiguration appConfig;

    @Autowired
    public DataRepositoryServiceApiController(
            ObjectMapper objectMapper,
            HttpServletRequest request,
            FileService fileService,
            ApplicationConfiguration appConfig
    ) {
        this.objectMapper = objectMapper;
        this.request = request;
        this.appConfig = appConfig;
        this.fileService = fileService;
    }

/*
    @InitBinder
    protected void initBinder(final WebDataBinder binder) {
        binder.addValidators();
    }
*/

    @Override
    public Optional<ObjectMapper> getObjectMapper() {
        return Optional.ofNullable(objectMapper);
    }

    @Override
    public Optional<HttpServletRequest> getRequest() {
        return Optional.ofNullable(request);
    }

    private AuthenticatedUserRequest getAuthenticatedInfo() {
        return AuthenticatedUserRequest.from(getRequest(), appConfig.getUserEmail());
    }

    @Override
    public ResponseEntity<DRSAccessURL> getAccessURL(@PathVariable("object_id") String objectId,
                                                     @PathVariable("access_id") String accessId) {
        // TODO: this has to return DRSError - not an ErrorModel
        // Have to figure out how to do that

        // We never give out access ids, so by definition, the input is invalid.
        throw new ValidationException("Invalid access_id: '" + accessId + "'");
    }

    @Override
    public ResponseEntity<DRSBundle> getBundle(@PathVariable("bundle_id") String bundleId) {
        // TODO: this has to return DRSError on error - not an ErrorModel
        return new ResponseEntity<>(fileService.lookupBundleByDrsId(bundleId), HttpStatus.OK);
    }

    @Override
    public ResponseEntity<DRSObject> getObject(@PathVariable("object_id") String objectId) {
        // TODO: this has to return DRSError on error - not an ErrorModel
        // The incoming object id is a DRS object id, not a file id.
        return new ResponseEntity<>(fileService.lookupObjectByDrsId(objectId), HttpStatus.OK);
    }

    @Override
    public ResponseEntity<DRSServiceInfo> getServiceInfo() {
        DRSServiceInfo info = new DRSServiceInfo()
            .version("0.0.1")
            .title("Jade")
            .description("Jade Data Repository - a Broad/Verily open source project")
            .contact("cbernard@broadinstitute.org")
            .license("Apache 2.0");
        return new ResponseEntity<>(info, HttpStatus.OK);
    }

}
