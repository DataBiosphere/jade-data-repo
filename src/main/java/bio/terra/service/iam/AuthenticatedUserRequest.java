package bio.terra.service.iam;

import bio.terra.service.iam.exception.IamUnauthorizedException;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public class AuthenticatedUserRequest {

    private String email;
    private String subjectId;
    private Optional<String> token;
    private UUID reqId;

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
            throw new IamUnauthorizedException("An OAuth token is required.");
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AuthenticatedUserRequest)) {
            return false;
        }
        AuthenticatedUserRequest that = (AuthenticatedUserRequest) o;
        return Objects.equals(getEmail(), that.getEmail()) &&
            Objects.equals(getSubjectId(), that.getSubjectId()) &&
            Objects.equals(getToken(), that.getToken()) &&
            Objects.equals(getReqId(), that.getReqId());
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
            .append(getEmail())
            .append(getSubjectId())
            .append(getToken())
            .append(getReqId())
            .toHashCode();
    }
}
