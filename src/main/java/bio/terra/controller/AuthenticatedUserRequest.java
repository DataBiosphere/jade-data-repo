package bio.terra.controller;

import bio.terra.exception.BadRequestException;

import javax.servlet.http.HttpServletRequest;
import java.util.Optional;
import java.util.UUID;

public class AuthenticatedUserRequest {

    private String email;
    private String token;
    private UUID reqId;

    public AuthenticatedUserRequest() {}

    public AuthenticatedUserRequest(String email, String token) {
        this.email = email;
        this.token = token;
        this.reqId = UUID.randomUUID();
    }

    public String getEmail() {
        return email;
    }

    public AuthenticatedUserRequest email(String email) {
        this.email = email;
        return this;
    }

    public String getToken() {
        return token;
    }

    public AuthenticatedUserRequest token(String token) {
        this.token = token;
        return this;
    }

    public UUID getReqId() {
        return reqId;
    }

    public AuthenticatedUserRequest reqId(UUID reqId) {
        this.reqId = reqId;
        return this;
    }

    // Static method to build an AuthenticatedUserRequest from data available to the controller
    public static AuthenticatedUserRequest from(Optional<HttpServletRequest> servletRequest,
                                                String appConfigUserEmail) {

        if (!servletRequest.isPresent()) {
            throw new BadRequestException("No valid request found.");
        }
        HttpServletRequest req = servletRequest.get();
        String email = req.getHeader("oidc_claim_email");
        String token = req.getHeader("oidc_access_token");

        if (token == null) {
            String authHeader = req.getHeader("Authorization");
            if (authHeader != null)
                token = authHeader.substring("Bearer ".length());
        }
        if (email == null) {
            email = appConfigUserEmail;
        }
        return new AuthenticatedUserRequest(email, token);
    }

}
