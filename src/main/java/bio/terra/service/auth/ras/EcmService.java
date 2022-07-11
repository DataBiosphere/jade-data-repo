package bio.terra.service.auth.ras;

import bio.terra.app.configuration.EcmConfiguration;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.externalcreds.api.PassportApi;
import bio.terra.externalcreds.client.ApiClient;
import bio.terra.externalcreds.model.RASv1Dot1VisaCriterion;
import bio.terra.externalcreds.model.ValidatePassportRequest;
import bio.terra.externalcreds.model.ValidatePassportResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.JWTParser;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
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
public class EcmService {
  private static final Logger logger = LoggerFactory.getLogger(EcmService.class);
  private final EcmConfiguration ecmConfiguration;
  private final RestTemplate restTemplate;
  private final ObjectMapper objectMapper;
  private final OidcApiService oidcApiService;

  private static final String RAS_PROVIDER = "ras";
  @VisibleForTesting public static final String GA4GH_PASSPORT_V1_CLAIM = "ga4gh_passport_v1";
  private static final String RAS_DBGAP_PERMISSIONS_CLAIM = "ras_dbgap_permissions";
  private static final String RAS_CRITERIA_TYPE = "RASv1Dot1VisaCriterion";

  @Autowired
  public EcmService(
      EcmConfiguration ecmConfiguration,
      RestTemplate restTemplate,
      OidcApiService oidcApiService,
      ObjectMapper objectMapper) {
    this.ecmConfiguration = ecmConfiguration;
    this.restTemplate = restTemplate;
    this.oidcApiService = oidcApiService;
    this.objectMapper = objectMapper;
  }

  public PassportApi getPassportApi() {
    var client = new ApiClient(restTemplate);
    client.setBasePath(ecmConfiguration.getBasePath());

    return new PassportApi(client);
  }

  public void addRasIssuerAndType(RASv1Dot1VisaCriterion criterion) {
    criterion.issuer(ecmConfiguration.getRasIssuer()).type(RAS_CRITERIA_TYPE);
  }

  public ValidatePassportResult validatePassport(ValidatePassportRequest validatePassportRequest) {
    var passportApi = getPassportApi();
    var result = passportApi.validatePassport(validatePassportRequest);

    if (result.isValid()) {
      var auditInfo = result.getAuditInfo();
      var df = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss z");
      df.setTimeZone(TimeZone.getTimeZone("UTC"));
      logger.info(
          """
              [Validate Passport Audit]:
              NIH User ID: {},
              Transaction Number: {},
              Data Repository accessed: {},
              Study/Data set accessed: {},
              Date/Time of access: {}
              """,
          auditInfo.getOrDefault("external_user_id", "not found"),
          auditInfo.getOrDefault("txn", "not found"),
          "TDR",
          ((RASv1Dot1VisaCriterion) result.getMatchedCriterion()).getPhsId(),
          df.format(new Date(System.currentTimeMillis())));
    }

    return result;
  }

  /**
   * @param userReq authenticated user
   * @return the user's linked RAS passport as a JWT, or null if none exists.
   */
  public String getRasProviderPassport(AuthenticatedUserRequest userReq) {
    try {
      String passport = oidcApiService.getOidcApi(userReq).getProviderPassport(RAS_PROVIDER);
      // Passports returned by OidcApi have a bug in their formatting:
      // double quotes must be stripped if passing back to PassportApi for validation,
      // otherwise the passport will not be considered valid JWT.
      // This stopgap can be removed when the client is fixed:
      // https://broadworkbench.atlassian.net/browse/ID-128
      return passport.replace("\"", "");
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
  public List<RasDbgapPermissions> getRasDbgapPermissions(AuthenticatedUserRequest userReq)
      throws ParseException {
    String passportJwt = getRasProviderPassport(userReq);
    if (!StringUtils.isEmpty(passportJwt)) {
      JWTClaimsSet claims = JWTParser.parse(passportJwt).getJWTClaimsSet();
      if (claims != null) {
        return ListUtils.emptyIfNull(claims.getStringListClaim(GA4GH_PASSPORT_V1_CLAIM)).stream()
            .flatMap(this::toRasDbgapPermissions)
            .toList();
      }
    }
    return List.of();
  }

  /**
   * @param visaJwt a passport's embedded access token
   * @return a stream of RAS dbGaP permissions constructed from the decoded visa, empty if it cannot
   *     be parsed.
   */
  private Stream<RasDbgapPermissions> toRasDbgapPermissions(String visaJwt) {
    try {
      Object claim =
          JWTParser.parse(visaJwt).getJWTClaimsSet().getClaim(RAS_DBGAP_PERMISSIONS_CLAIM);
      return Arrays.stream(objectMapper.convertValue(claim, RasDbgapPermissions[].class));
    } catch (IllegalArgumentException | ParseException ex) {
      String msg =
          String.format(
              "Error parsing RAS passport's decoded visa's %s", RAS_DBGAP_PERMISSIONS_CLAIM);
      logger.warn(msg, ex);
    }
    return Stream.empty();
  }
}
