package bio.terra.controller;

import bio.terra.exception.BadRequestException;

import javax.servlet.http.HttpServletRequest;
import java.util.Optional;
import java.util.UUID;

public class AuthenticatedUserRequest {

    private String email;

    private String subjectId;
    private String token;
    private UUID reqId;

    public AuthenticatedUserRequest() {
        this.reqId = UUID.randomUUID();
    }

    public AuthenticatedUserRequest(String email, String subjectId, String token) {
        this.email = email;
        this.subjectId = subjectId;
        this.token = token;
    }

    public String getSubjectId() {
        return subjectId;
    }

    public AuthenticatedUserRequest subjectId(String subjectId) {
        this.subjectId = subjectId;
        return this;
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
        String userId = req.getHeader("oidc_claim_user_id");

        if (token == null) {
            String authHeader = req.getHeader("Authorization");
            if (authHeader != null)
                token = authHeader.substring("Bearer ".length());
        }
        if (email == null) {
            String fromHeader = req.getHeader("From");
            if (fromHeader != null) {
                email = fromHeader;
            } else {
                email = appConfigUserEmail;
            }
        }
        if (userId == null) {
            userId = "999999999999";
        }
        return new AuthenticatedUserRequest().email(email).subjectId(userId).token(token);
    }

}
