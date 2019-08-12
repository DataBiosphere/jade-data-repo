package bio.terra.controller;

import bio.terra.configuration.ApplicationConfiguration;
import bio.terra.exception.BadRequestException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import java.util.Optional;

@Primary
@Profile("!terra")
@Component
public class LocalAuthenticatedUserRequestFactory implements AuthenticatedUserRequestFactory {
    private final ApplicationConfiguration applicationConfiguration;

    @Autowired
    public LocalAuthenticatedUserRequestFactory(ApplicationConfiguration applicationConfiguration) {
        this.applicationConfiguration = applicationConfiguration;
    }

    // Static method to build an AuthenticatedUserRequest from data available to the controller
    public AuthenticatedUserRequest from(HttpServletRequest servletRequest) {

        HttpServletRequest req = servletRequest;
        String email = applicationConfiguration.getUserEmail();
        String header = Optional.of(req.getHeader("Authorization"))
            .orElseThrow(() -> new BadRequestException("No Authorization Header found."));
        String token = header.substring("Bearer ".length());

        return new AuthenticatedUserRequest(email, token);
    }


}
