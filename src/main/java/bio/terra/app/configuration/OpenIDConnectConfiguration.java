package bio.terra.app.configuration;

import bio.terra.common.exception.ServiceInitializationException;
import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.List;
import javax.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.RestTemplate;

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "oidc")
public class OpenIDConnectConfiguration {
  private static final String OIDC_METADATA_URL_SUFFIX = ".well-known/openid-configuration";
  private final RestTemplate oidcRestTemplate;

  private String schemeName;
  private String authorityEndpoint;
  private String clientId;
  private String clientSecret;
  private String extraAuthParams;
  private boolean addClientIdToScope;

  private String authorizationEndpoint;
  private String tokenEndpoint;

  @Autowired
  public OpenIDConnectConfiguration() {
    this.oidcRestTemplate = new RestTemplate(new HttpComponentsClientHttpRequestFactory());
    MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
    converter.setSupportedMediaTypes(Collections.singletonList(MediaType.ALL));
    oidcRestTemplate.setMessageConverters(List.of(converter));
  }

  @PostConstruct
  @VisibleForTesting
  void init() {
    if (!StringUtils.isEmpty(getAuthorityEndpoint())) {
      HttpHeaders headers = new HttpHeaders();
      headers.setContentType(MediaType.APPLICATION_JSON);

      ResponseEntity<OpenIDProviderMetadata> metadataConfig =
          oidcRestTemplate.exchange(
              getOidcMetadataUrl(),
              HttpMethod.GET,
              new HttpEntity<>(null, headers),
              OpenIDProviderMetadata.class);
      if (!metadataConfig.getStatusCode().is2xxSuccessful()) {
        throw new ServiceInitializationException(
            String.format(
                "Error reading OIDC configuration endpoint: %s",
                metadataConfig.getStatusCode().getReasonPhrase()));
      }
      OpenIDProviderMetadata response = metadataConfig.getBody();
      if (response == null) {
        throw new ServiceInitializationException(
            "Error reading OIDC configuration." + " An empty response was returned");
      }
      if (response.getAuthorizationEndpoint() == null) {
        throw new ServiceInitializationException(
            "Authorization endpoint needs to be provided by provider metadata endpoint");
      }
      authorizationEndpoint = response.getAuthorizationEndpoint();

      if (response.getTokenEndpoint() == null) {
        throw new ServiceInitializationException(
            "Token endpoint needs to be provided by provider metadata endpoint");
      }
      tokenEndpoint = response.getTokenEndpoint();
    }
  }

  private String getOidcMetadataUrl() {
    return getAuthorityEndpoint() + "/" + OIDC_METADATA_URL_SUFFIX;
  }

  public String getSchemeName() {
    return schemeName;
  }

  public void setSchemeName(String schemeName) {
    this.schemeName = schemeName;
  }

  public String getAuthorityEndpoint() {
    return authorityEndpoint;
  }

  public void setAuthorityEndpoint(String authorityEndpoint) {
    this.authorityEndpoint = authorityEndpoint;
  }

  public String getClientId() {
    return clientId;
  }

  public void setClientId(String clientId) {
    this.clientId = clientId;
  }

  public String getClientSecret() {
    return clientSecret;
  }

  public void setClientSecret(String clientSecret) {
    this.clientSecret = clientSecret;
  }

  public String getExtraAuthParams() {
    return extraAuthParams;
  }

  public void setExtraAuthParams(String extraAuthParams) {
    this.extraAuthParams = extraAuthParams;
  }

  public boolean isAddClientIdToScope() {
    return addClientIdToScope;
  }

  public void setAddClientIdToScope(Boolean addClientIdToScope) {
    this.addClientIdToScope = addClientIdToScope;
  }

  public String getAuthorizationEndpoint() {
    return authorizationEndpoint;
  }

  public String getTokenEndpoint() {
    return tokenEndpoint;
  }
}
