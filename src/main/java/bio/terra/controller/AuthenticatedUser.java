package bio.terra.controller;

import bio.terra.exception.BadRequestException;

import javax.servlet.http.HttpServletRequest;
import java.util.Optional;
import java.util.UUID;

public class AuthenticatedUser implements UserInfo {

    private String email;
    private String subjectId;
    private String token;
    private UUID reqId;

    public AuthenticatedUser() {
        this.reqId = UUID.randomUUID();
    }

    public AuthenticatedUser(String email, String subjectId, String token) {
        this.email = email;
        this.subjectId = subjectId;
        this.token = token;
    }

    @Override
    public String getSubjectId() {
        return subjectId;
    }

    @Override
    public AuthenticatedUser subjectId(String subjectId) {
        this.subjectId = subjectId;
        return this;
    }

    @Override
    public String getEmail() {
        return email;
    }

    @Override
    public AuthenticatedUser email(String email) {
        this.email = email;
        return this;
    }

    public String getToken() {
        return token;
    }

    public AuthenticatedUser token(String token) {
        this.token = token;
        return this;
    }

    public UUID getReqId() {
        return reqId;
    }

    public AuthenticatedUser reqId(UUID reqId) {
        this.reqId = reqId;
        return this;
    }

    // Static method to build an AuthenticatedUser from data available to the controller
    public static AuthenticatedUser from(Optional<HttpServletRequest> servletRequest,
                                         String appConfigUserEmail) {

        if (!servletRequest.isPresent()) {
            throw new BadRequestException("No valid request found.");
        }
        HttpServletRequest req = servletRequest.get();
        String email = req.getHeader("oidc_claim_email");
        String token = req.getHeader("oidc_access_token");
        String userId = req.getHeader("oidc_claim_user_id");

        // in testing scenarios and when running the server without the proxy not all the
        // header information will be available. default values will be used in these cases.

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
        return new AuthenticatedUser().email(email).subjectId(userId).token(token);
    }

}
