package bio.terra.app.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Provider metadata as specified in the OpenId discover spec: <a
 * href="https://openid.net/specs/openid-connect-discovery-1_0.html#rfc.section.3">
 * https://openid.net/specs/openid-connect-discovery-1_0.html#rfc.section.3 </a>
 */
public class OpenIDProviderMetadata {
  private final String issuer;
  private final String authorizationEndpoint;
  private final String tokenEndpoint;
  private final String userinfoEndpoint;
  private final String jwksUri;
  private final String registrationEndpoint;
  private final List<String> scopesSupported;
  private final List<String> responseTypesSupported;
  private final List<String> grantTypesSupported;
  private final List<String> acrValuesSupported;
  private final List<String> subjectTypesSupported;
  private final List<String> idTokenSigningAlgValuesSupported;
  private final List<String> idTokenEncryptionAlgValuesSupported;
  private final List<String> idTokenEncryptionEncValuesSupported;
  private final List<String> userinfoSigningAlgValuesSupported;
  private final List<String> userinfoEncryptionAlgValuesSupported;
  private final List<String> userinfoEncryptionEncValuesSupported;
  private final List<String> requestObjectSigningAlgValuesSupported;
  private final List<String> requestObjectEncryptionAlgValuesSupported;
  private final List<String> requestObjectEncryptionEncValuesSupported;
  private final List<String> tokenEndpointAuthMethodsSupported;
  private final List<String> tokenEndpointAuthSigningAlgValuesSupported;
  private final List<String> displayValuesSupported;
  private final List<String> claimTypesSupported;
  private final List<String> claimsSupported;
  private final String serviceDocumentation;
  private final List<String> claimsLocalesSupported;
  private final List<String> uiLocalesSupported;
  private final Boolean claimsParameterSupported;
  private final Boolean requestParameterSupported;
  private final Boolean requestUriParameterSupported;
  private final Boolean requireRequestUriRegistration;
  private final String opPolicyUri;
  private final String opTosUri;

  @JsonCreator
  public OpenIDProviderMetadata(
      @JsonProperty(value = "issuer", required = true) String issuer,
      @JsonProperty(value = "authorization_endpoint", required = true) String authorizationEndpoint,
      @JsonProperty(value = "token_endpoint") String tokenEndpoint,
      @JsonProperty(value = "userinfo_endpoint") String userinfoEndpoint,
      @JsonProperty(value = "jwks_uri", required = true) String jwksUri,
      @JsonProperty(value = "registration_endpoint") String registrationEndpoint,
      @JsonProperty(value = "scopes_supported") List<String> scopesSupported,
      @JsonProperty(value = "response_types_supported", required = true)
          List<String> responseTypesSupported,
      @JsonProperty(value = "grant_types_supported") List<String> grantTypesSupported,
      @JsonProperty(value = "acr_values_supported") List<String> acrValuesSupported,
      @JsonProperty(value = "subject_types_supported", required = true)
          List<String> subjectTypesSupported,
      @JsonProperty(value = "id_token_signing_alg_values_supported", required = true)
          List<String> idTokenSigningAlgValuesSupported,
      @JsonProperty(value = "id_token_encryption_alg_values_supported")
          List<String> idTokenEncryptionAlgValuesSupported,
      @JsonProperty(value = "id_token_encryption_enc_values_supported")
          List<String> idTokenEncryptionEncValuesSupported,
      @JsonProperty(value = "userinfo_signing_alg_values_supported")
          List<String> userinfoSigningAlgValuesSupported,
      @JsonProperty(value = "userinfo_encryption_alg_values_supported")
          List<String> userinfoEncryptionAlgValuesSupported,
      @JsonProperty(value = "userinfo_encryption_enc_values_supported")
          List<String> userinfoEncryptionEncValuesSupported,
      @JsonProperty(value = "request_object_signing_alg_values_supported")
          List<String> requestObjectSigningAlgValuesSupported,
      @JsonProperty(value = "request_object_encryption_alg_values_supported")
          List<String> requestObjectEncryptionAlgValuesSupported,
      @JsonProperty(value = "request_object_encryption_enc_values_supported")
          List<String> requestObjectEncryptionEncValuesSupported,
      @JsonProperty(value = "token_endpoint_auth_methods_supported")
          List<String> tokenEndpointAuthMethodsSupported,
      @JsonProperty(value = "token_endpoint_auth_signing_alg_values_supported")
          List<String> tokenEndpointAuthSigningAlgValuesSupported,
      @JsonProperty(value = "display_values_supported") List<String> displayValuesSupported,
      @JsonProperty(value = "claim_types_supported") List<String> claimTypesSupported,
      @JsonProperty(value = "claims_supported") List<String> claimsSupported,
      @JsonProperty(value = "service_documentation") String serviceDocumentation,
      @JsonProperty(value = "claims_locales_supported") List<String> claimsLocalesSupported,
      @JsonProperty(value = "ui_locales_supported") List<String> uiLocalesSupported,
      @JsonProperty(value = "claims_parameter_supported") Boolean claimsParameterSupported,
      @JsonProperty(value = "request_parameter_supported") Boolean requestParameterSupported,
      @JsonProperty(value = "request_uri_parameter_supported") Boolean requestUriParameterSupported,
      @JsonProperty(value = "require_request_uri_registration")
          Boolean requireRequestUriRegistration,
      @JsonProperty(value = "op_policy_uri") String opPolicyUri,
      @JsonProperty(value = "op_tos_uri") String opTosUri) {
    this.issuer = issuer;
    this.authorizationEndpoint = authorizationEndpoint;
    this.tokenEndpoint = tokenEndpoint;
    this.userinfoEndpoint = userinfoEndpoint;
    this.jwksUri = jwksUri;
    this.registrationEndpoint = registrationEndpoint;
    this.scopesSupported = scopesSupported;
    this.responseTypesSupported = responseTypesSupported;
    this.grantTypesSupported = grantTypesSupported;
    this.acrValuesSupported = acrValuesSupported;
    this.subjectTypesSupported = subjectTypesSupported;
    this.idTokenSigningAlgValuesSupported = idTokenSigningAlgValuesSupported;
    this.idTokenEncryptionAlgValuesSupported = idTokenEncryptionAlgValuesSupported;
    this.idTokenEncryptionEncValuesSupported = idTokenEncryptionEncValuesSupported;
    this.userinfoSigningAlgValuesSupported = userinfoSigningAlgValuesSupported;
    this.userinfoEncryptionAlgValuesSupported = userinfoEncryptionAlgValuesSupported;
    this.userinfoEncryptionEncValuesSupported = userinfoEncryptionEncValuesSupported;
    this.requestObjectSigningAlgValuesSupported = requestObjectSigningAlgValuesSupported;
    this.requestObjectEncryptionAlgValuesSupported = requestObjectEncryptionAlgValuesSupported;
    this.requestObjectEncryptionEncValuesSupported = requestObjectEncryptionEncValuesSupported;
    this.tokenEndpointAuthMethodsSupported = tokenEndpointAuthMethodsSupported;
    this.tokenEndpointAuthSigningAlgValuesSupported = tokenEndpointAuthSigningAlgValuesSupported;
    this.displayValuesSupported = displayValuesSupported;
    this.claimTypesSupported = claimTypesSupported;
    this.claimsSupported = claimsSupported;
    this.serviceDocumentation = serviceDocumentation;
    this.claimsLocalesSupported = claimsLocalesSupported;
    this.uiLocalesSupported = uiLocalesSupported;
    this.claimsParameterSupported = claimsParameterSupported;
    this.requestParameterSupported = requestParameterSupported;
    this.requestUriParameterSupported = requestUriParameterSupported;
    this.requireRequestUriRegistration = requireRequestUriRegistration;
    this.opPolicyUri = opPolicyUri;
    this.opTosUri = opTosUri;
  }

  public String getIssuer() {
    return issuer;
  }

  public String getAuthorizationEndpoint() {
    return authorizationEndpoint;
  }

  public String getTokenEndpoint() {
    return tokenEndpoint;
  }

  public String getUserinfoEndpoint() {
    return userinfoEndpoint;
  }

  public String getJwksUri() {
    return jwksUri;
  }

  public String getRegistrationEndpoint() {
    return registrationEndpoint;
  }

  public List<String> getScopesSupported() {
    return scopesSupported;
  }

  public List<String> getResponseTypesSupported() {
    return responseTypesSupported;
  }

  public List<String> getGrantTypesSupported() {
    return grantTypesSupported;
  }

  public List<String> getAcrValuesSupported() {
    return acrValuesSupported;
  }

  public List<String> getSubjectTypesSupported() {
    return subjectTypesSupported;
  }

  public List<String> getIdTokenSigningAlgValuesSupported() {
    return idTokenSigningAlgValuesSupported;
  }

  public List<String> getIdTokenEncryptionAlgValuesSupported() {
    return idTokenEncryptionAlgValuesSupported;
  }

  public List<String> getIdTokenEncryptionEncValuesSupported() {
    return idTokenEncryptionEncValuesSupported;
  }

  public List<String> getUserinfoSigningAlgValuesSupported() {
    return userinfoSigningAlgValuesSupported;
  }

  public List<String> getUserinfoEncryptionAlgValuesSupported() {
    return userinfoEncryptionAlgValuesSupported;
  }

  public List<String> getUserinfoEncryptionEncValuesSupported() {
    return userinfoEncryptionEncValuesSupported;
  }

  public List<String> getRequestObjectSigningAlgValuesSupported() {
    return requestObjectSigningAlgValuesSupported;
  }

  public List<String> getRequestObjectEncryptionAlgValuesSupported() {
    return requestObjectEncryptionAlgValuesSupported;
  }

  public List<String> getRequestObjectEncryptionEncValuesSupported() {
    return requestObjectEncryptionEncValuesSupported;
  }

  public List<String> getTokenEndpointAuthMethodsSupported() {
    return tokenEndpointAuthMethodsSupported;
  }

  public List<String> getTokenEndpointAuthSigningAlgValuesSupported() {
    return tokenEndpointAuthSigningAlgValuesSupported;
  }

  public List<String> getDisplayValuesSupported() {
    return displayValuesSupported;
  }

  public List<String> getClaimTypesSupported() {
    return claimTypesSupported;
  }

  public List<String> getClaimsSupported() {
    return claimsSupported;
  }

  public String getServiceDocumentation() {
    return serviceDocumentation;
  }

  public List<String> getClaimsLocalesSupported() {
    return claimsLocalesSupported;
  }

  public List<String> getUiLocalesSupported() {
    return uiLocalesSupported;
  }

  public Boolean getClaimsParameterSupported() {
    return claimsParameterSupported;
  }

  public Boolean getRequestParameterSupported() {
    return requestParameterSupported;
  }

  public Boolean getRequestUriParameterSupported() {
    return requestUriParameterSupported;
  }

  public Boolean getRequireRequestUriRegistration() {
    return requireRequestUriRegistration;
  }

  public String getOpPolicyUri() {
    return opPolicyUri;
  }

  public String getOpTosUri() {
    return opTosUri;
  }
}
