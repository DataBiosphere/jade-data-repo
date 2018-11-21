package bio.terra.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import javax.servlet.http.HttpServletRequest;
import java.util.Optional;
@javax.annotation.Generated(value = "io.swagger.codegen.languages.SpringCodegen", date = "2018-11-21T14:51:14.798-05:00")

@Controller
public class UnauthenticatedApiController implements UnauthenticatedApi {

    private final ObjectMapper objectMapper;

    private final HttpServletRequest request;

    @Autowired
    public UnauthenticatedApiController(ObjectMapper objectMapper, HttpServletRequest request) {
        this.objectMapper = objectMapper;
        this.request = request;
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
    @RequestMapping(value = "/status",
            method = RequestMethod.GET)
    public ResponseEntity<Void> serviceStatus() {
        return new ResponseEntity<>(HttpStatus.OK);
    }

}
