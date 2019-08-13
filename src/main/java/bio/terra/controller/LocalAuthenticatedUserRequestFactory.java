package bio.terra.controller;

import bio.terra.configuration.ApplicationConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private Logger logger = LoggerFactory.getLogger(LocalAuthenticatedUserRequestFactory.class);

    private final ApplicationConfiguration applicationConfiguration;

    @Autowired
    public LocalAuthenticatedUserRequestFactory(ApplicationConfiguration applicationConfiguration) {
        this.applicationConfiguration = applicationConfiguration;
    }

    // Static method to build an AuthenticatedUserRequest from data available to the controller
    public AuthenticatedUserRequest from(HttpServletRequest servletRequest) {
        HttpServletRequest req = servletRequest;
        String email = applicationConfiguration.getUserEmail();

        Optional<String> token = Optional.ofNullable(req.getHeader("Authorization"))
            .map(header -> header.substring("Bearer ".length()));

        return new AuthenticatedUserRequest(email, token);
    }

}
