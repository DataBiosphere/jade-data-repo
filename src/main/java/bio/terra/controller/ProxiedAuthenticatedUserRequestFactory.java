package bio.terra.controller;

import bio.terra.exception.BadRequestException;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import java.util.Optional;

@Profile({"terra", "dev", "integration"})
@Component
public class ProxiedAuthenticatedUserRequestFactory implements AuthenticatedUserRequestFactory {

    // Method to build an AuthenticatedUserRequest from data available to the controller
    public AuthenticatedUserRequest from(HttpServletRequest servletRequest) {
        HttpServletRequest req = servletRequest;
        String email = Optional.of(req.getHeader("oidc_claim_email"))
            .orElseThrow(() -> new BadRequestException("No valid email found in oidc_claim_email header."));
        String token = Optional.of(req.getHeader("oidc_access_token"))
            .orElseThrow(() -> new BadRequestException("No valid token found in oidc_access_token header."));

        return new AuthenticatedUserRequest(email, Optional.of(token));
    }

}
