package bio.terra.service.auth.ras;

import bio.terra.app.configuration.ECMConfiguration;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.externalcreds.api.OidcApi;
import bio.terra.externalcreds.api.PassportApi;
import bio.terra.externalcreds.client.ApiClient;
import bio.terra.externalcreds.model.ValidatePassportRequest;
import bio.terra.externalcreds.model.ValidatePassportResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.JWTParser;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import liquibase.util.StringUtils;
import org.apache.commons.collections4.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

@Service
public class ECMService {
  private final Logger logger = LoggerFactory.getLogger(ECMService.class);
  private final ECMConfiguration ecmConfiguration;
  private final RestTemplate restTemplate;
  private final ObjectMapper objectMapper;

  private static final String RAS_PROVIDER = "ras";
  private static final String GA4GH_PASSPORT_V1_CLAIM = "ga4gh_passport_v1";
  private static final String RAS_DBGAP_PERMISSIONS_CLAIM = "ras_dbgap_permissions";

  @Autowired
  public ECMService(
      ECMConfiguration ecmConfiguration, RestTemplate restTemplate, ObjectMapper objectMapper) {
    this.ecmConfiguration = ecmConfiguration;
    this.restTemplate = restTemplate;
    this.objectMapper = objectMapper;
  }

  public PassportApi getPassportApi() {
    var client = new ApiClient(restTemplate);
    client.setBasePath(ecmConfiguration.getBasePath());

    return new PassportApi(client);
  }

  public ValidatePassportResult validatePassport(ValidatePassportRequest validatePassportRequest) {
    var passportApi = getPassportApi();
    return passportApi.validatePassport(validatePassportRequest);
  }

  public OidcApi getOidcApi(AuthenticatedUserRequest userReq) {
    var client = new ApiClient(restTemplate);
    client.setBasePath(ecmConfiguration.getBasePath());
    client.setAccessToken(userReq.getToken());

    return new OidcApi(client);
  }

  /**
   * @param userReq authenticated user
   * @return the user's linked RAS passport as a JWT, or null if none exists.
   */
  private String getRASProviderPassport(AuthenticatedUserRequest userReq) {
    try {
      return getOidcApi(userReq).getProviderPassport(RAS_PROVIDER);
    } catch (HttpClientErrorException ex) {
      if (ex.getStatusCode() == HttpStatus.NOT_FOUND) {
        return null;
      }
      throw ex;
    }
  }

  /**
   * @param userReq authenticated user
   * @return RAS dbGaP permissions attributed to the user's linked RAS passport, or an empty list if
   *     none exists.
   * @throws ParseException
   */
  public List<RASDbgapPermissions> getRASDbgapPermissions(AuthenticatedUserRequest userReq)
      throws ParseException {
    String passportJwt = getRASProviderPassport(userReq);
    if (!StringUtils.isEmpty(passportJwt)) {
      JWTClaimsSet claims = JWTParser.parse(passportJwt).getJWTClaimsSet();
      if (claims != null) {
        return ListUtils.emptyIfNull(claims.getStringListClaim(GA4GH_PASSPORT_V1_CLAIM)).stream()
            .flatMap(this::toRASDbgapPermissions)
            .toList();
      }
    }
    return List.of();
  }

  /**
   * @param visaJwt a passport's embedded access token
   * @return a stream of RASDbgapPermissions constructed from the decoded visa, empty if it cannot
   *     be parsed.
   */
  private Stream<RASDbgapPermissions> toRASDbgapPermissions(String visaJwt) {
    try {
      Object claim =
          JWTParser.parse(visaJwt).getJWTClaimsSet().getClaim(RAS_DBGAP_PERMISSIONS_CLAIM);
      return Arrays.stream(objectMapper.convertValue(claim, RASDbgapPermissions[].class));
    } catch (ParseException ex) {
      logger.warn(String.format("Error parsing RAS passport %s", RAS_DBGAP_PERMISSIONS_CLAIM), ex);
    }
    return Stream.empty();
  }
}
