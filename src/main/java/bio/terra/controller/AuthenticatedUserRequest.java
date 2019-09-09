package bio.terra.controller;

import bio.terra.controller.exception.ApiException;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Optional;
import java.util.UUID;

public class AuthenticatedUserRequest {

    private String email;
    private String subjectId;
    private Optional<String> token;
    private UUID reqId;
    private boolean canListJobs;
    private boolean canDeleteJobs;

    public AuthenticatedUserRequest() {
        this.reqId = UUID.randomUUID();
    }

    public AuthenticatedUserRequest(String email, String subjectId, Optional<String> token) {
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

    public Optional<String> getToken() {
        return token;
    }

    public AuthenticatedUserRequest token(Optional<String> token) {
        this.token = token;
        return this;
    }

    @JsonIgnore
    public String getRequiredToken() {
        if (!token.isPresent()) {
            throw new ApiException("Token required");
        }
        return token.get();
    }

    public UUID getReqId() {
        return reqId;
    }

    public AuthenticatedUserRequest reqId(UUID reqId) {
        this.reqId = reqId;
        return this;
    }

    public boolean getCanListJobs() {
        return canListJobs;
    }

    public AuthenticatedUserRequest canListJobs(boolean canListJobs) {
        this.canListJobs = canListJobs;
        return this;
    }

    public boolean getCanDeleteJobs() {
        return canDeleteJobs;
    }

    public AuthenticatedUserRequest canDeleteJobs(boolean canDeleteJobs) {
        this.canDeleteJobs = canDeleteJobs;
        return this;
    }

}
