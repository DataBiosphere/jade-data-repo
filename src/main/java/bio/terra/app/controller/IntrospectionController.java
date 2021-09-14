package bio.terra.app.controller;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;
import okhttp3.Request.Builder;
import okhttp3.Response;
import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.apache.commons.collections4.map.PassiveExpiringMap.ExpirationPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * This class is here to demonstrate how we could verify Google opaque tokens.
 * https://datatracker.ietf.org/doc/html/rfc7662
 */
@Controller
public class IntrospectionController {

  @Autowired private ObjectMapper objectMapper;

  private final OkHttpClient httpClient =
      new OkHttpClient.Builder().readTimeout(5, TimeUnit.SECONDS).build();
  private final Map<String, IntrospectionResponse> introspectionCache =
      Collections.synchronizedMap(
          new PassiveExpiringMap<>(
              (ExpirationPolicy<String, IntrospectionResponse>)
                  (key, value) -> value.getExpiresIn() * 1000L));

  @RequestMapping(
      value = "/introspect",
      produces = {"application/json"},
      consumes = {"application/x-www-form-urlencoded"},
      method = RequestMethod.POST)
  public ResponseEntity<IntrospectionResponse> introspect(
      @RequestParam(value = "token") String token) {

    return ResponseEntity.ok(introspectionCache.computeIfAbsent(token, this::doIntrospect));
  }

  private IntrospectionResponse doIntrospect(String token) {
    try {
      Response response =
          httpClient
              .newCall(
                  new Builder()
                      .url(
                          String.format(
                              "https://www.googleapis.com/oauth2/v1/tokeninfo?access_token=%s",
                              token))
                      .get()
                      .build())
              .execute();
      if (response.code() > 300) {
        response.body().byteStream().close();
        return new IntrospectionResponse(false, null, null, null, null, null, null, null, 0);
      }
      Map<String, String> rawResponse =
          objectMapper.readValue(response.body().string(), new TypeReference<>() {});
      int expiresIn = Integer.parseInt(rawResponse.get("expires_in"));
      return new IntrospectionResponse(
          true,
          rawResponse.get("scope"),
          rawResponse.get("user_id"),
          rawResponse.get("email"),
          rawResponse.get("email"),
          Instant.now().getEpochSecond() + expiresIn,
          "bearer",
          rawResponse.get("audience"),
          expiresIn);
    } catch (IOException e) {
      throw new RuntimeException("Failed to reads user info", e);
    }
  }

  public static class IntrospectionResponse {
    private final boolean active;
    private final String scope;
    private final String sub;
    private final String username;
    private final String email;
    private final Long exp;
    private final String token_type;
    private final String aud;
    private final int expiresIn;

    @JsonCreator
    public IntrospectionResponse(
        @JsonProperty("active") boolean active,
        @JsonProperty("scope") String scope,
        @JsonProperty("sub") String sub,
        @JsonProperty("username") String username,
        @JsonProperty("email") String email,
        @JsonProperty("exp") Long exp,
        @JsonProperty("token_type") String token_type,
        @JsonProperty("aud") String aud,
        int expiresIn) {
      this.active = active;
      this.scope = scope;
      this.sub = sub;
      this.username = username;
      this.email = email;
      this.exp = exp;
      this.token_type = token_type;
      this.aud = aud;
      this.expiresIn = expiresIn;
    }

    public boolean isActive() {
      return active;
    }

    public String getScope() {
      return scope;
    }

    public String getSub() {
      return sub;
    }

    public String getUsername() {
      return username;
    }

    public String getEmail() {
      return email;
    }

    public Long getExp() {
      return exp;
    }

    public String getToken_type() {
      return token_type;
    }

    public String getAud() {
      return aud;
    }

    @JsonIgnore
    public int getExpiresIn() {
      return expiresIn;
    }
  }
}
