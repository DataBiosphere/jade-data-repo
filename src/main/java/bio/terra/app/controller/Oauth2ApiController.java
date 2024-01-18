package bio.terra.app.controller;

import bio.terra.app.configuration.OpenIDConnectConfiguration;
import bio.terra.common.exception.BadRequestException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.api.client.util.Charsets;
import jakarta.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.client.RestTemplate;

@Controller
public class Oauth2ApiController {
  static final String AUTHORIZE_ENDPOINT = "/oauth2/authorize";
  static final String TOKEN_REFRESH_ENDPOINT = "/oauth2/token";
  private static final String SCOPE_PARAM = "scope";
  private static final String CLIENT_SECRET_PARAM = "client_secret";
  private final OpenIDConnectConfiguration openIDConnectConfiguration;
  private final RestTemplate restTemplate;

  @Autowired
  public Oauth2ApiController(
      OpenIDConnectConfiguration openIDConnectConfiguration, RestTemplate restTemplate) {
    this.openIDConnectConfiguration = openIDConnectConfiguration;
    this.restTemplate = restTemplate;
  }

  @RequestMapping(value = AUTHORIZE_ENDPOINT)
  public String oauthAuthorize(HttpServletRequest request) {
    String concatCharacter =
        openIDConnectConfiguration.getAuthorizationEndpoint().contains("?") ? "&" : "?";
    return "redirect:"
        + openIDConnectConfiguration.getAuthorizationEndpoint()
        + concatCharacter
        + decorateAuthRedirectQueryParameters(request.getQueryString());
  }

  // Modify the query parameters that were passed to be compatible with configured Oauth flow
  private String decorateAuthRedirectQueryParameters(String initialParams) {
    List<NameValuePair> parameters = URLEncodedUtils.parse(initialParams, Charsets.UTF_8);

    parameters.addAll(
        URLEncodedUtils.parse(openIDConnectConfiguration.getExtraAuthParams(), Charsets.UTF_8));

    parameters =
        parameters.stream()
            .map(
                p -> {
                  if (openIDConnectConfiguration.isAddClientIdToScope()
                      && p.getName().equalsIgnoreCase(SCOPE_PARAM)) {
                    return new BasicNameValuePair(
                        p.getName(), p.getValue() + " " + openIDConnectConfiguration.getClientId());
                  } else {
                    return p;
                  }
                })
            .toList();

    return URLEncodedUtils.format(parameters, Charsets.UTF_8);
  }

  @RequestMapping(
      value = TOKEN_REFRESH_ENDPOINT,
      method = RequestMethod.POST,
      consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<JsonNode> oauthTokenPost(HttpServletRequest request) {
    String queryString =
        StringUtils.isEmpty(request.getQueryString()) ? "" : "?" + request.getQueryString();
    String actualEndpoint = openIDConnectConfiguration.getTokenEndpoint() + queryString;

    String requestBody;
    try (BufferedReader reader = request.getReader()) {
      requestBody = addClientSecret(IOUtils.toString(reader));
    } catch (IOException e) {
      throw new BadRequestException("Could not process refresh request", e);
    }

    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

    return restTemplate.exchange(
        actualEndpoint, HttpMethod.POST, new HttpEntity<>(requestBody, headers), JsonNode.class);
  }

  // Modify the form url encoded body and add the client secret if it was not specified
  private String addClientSecret(String requestBody) {
    List<NameValuePair> parameters = URLEncodedUtils.parse(requestBody, Charsets.UTF_8);

    if (!StringUtils.isEmpty(openIDConnectConfiguration.getClientSecret())
        && parameters.stream()
            .noneMatch(
                p ->
                    p.getName().equalsIgnoreCase(CLIENT_SECRET_PARAM)
                        && !StringUtils.isEmpty(p.getValue()))) {
      parameters.add(
          new BasicNameValuePair(
              CLIENT_SECRET_PARAM, openIDConnectConfiguration.getClientSecret()));
    }

    return URLEncodedUtils.format(parameters, Charsets.UTF_8);
  }
}
