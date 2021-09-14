package bio.terra.service.iam;

import bio.terra.service.iam.exception.IamUnauthorizedException;
import com.auth0.jwt.JWT;
import com.auth0.jwt.exceptions.JWTDecodeException;
import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.lang.builder.HashCodeBuilder;

public class AuthenticatedUserRequest {
  // key in JWT token that contains a google acess token
  private static final String ACCESS_TOKEN_CLAIM = "third_party_access_token";
  private String email;
  private String subjectId;
  private Optional<String> token = Optional.empty();
  private Optional<DecodedJWT> jwt = Optional.empty();
  private UUID reqId;

  public AuthenticatedUserRequest() {
    this.reqId = UUID.randomUUID();
  }

  public AuthenticatedUserRequest(String email, String subjectId, Optional<String> token) {
    this.email = email;
    this.subjectId = subjectId;
    this.token(token);
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

  public synchronized AuthenticatedUserRequest token(Optional<String> token) {
    this.token = token;
    this.jwt =
        token.flatMap(
            t -> {
              try {
                return Optional.of(JWT.decode(t));
              } catch (JWTDecodeException e) {
                return Optional.empty();
              }
            });
    return this;
  }

  public Optional<String> getAccessToken() {
    // When a token is present but a jwt is not, assume a Google opaque token
    if (token.isPresent() && jwt.isEmpty()) {
      return token;
    }
    return jwt.flatMap(
        j -> {
          Claim claim = j.getClaim(ACCESS_TOKEN_CLAIM);
          if (!claim.isNull()) {
            return Optional.of(claim.asString());
          } else {
            return Optional.empty();
          }
        });
  }

  public Optional<DecodedJWT> getJwt() {
    return jwt;
  }

  @JsonIgnore
  public String getRequiredAccessToken() {
    return getAccessToken()
        .orElseThrow(() -> new IamUnauthorizedException("An OAuth access token is required."));
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
    return Objects.equals(getEmail(), that.getEmail())
        && Objects.equals(getSubjectId(), that.getSubjectId())
        && Objects.equals(getToken(), that.getToken())
        && Objects.equals(getJwt(), that.getJwt())
        && Objects.equals(getReqId(), that.getReqId());
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(getEmail())
        .append(getSubjectId())
        .append(getToken())
        .append(getJwt())
        .append(getReqId())
        .toHashCode();
  }
}
