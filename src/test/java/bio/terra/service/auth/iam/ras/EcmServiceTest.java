package bio.terra.service.auth.iam.ras;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import bio.terra.app.configuration.EcmConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.externalcreds.api.OidcApi;
import bio.terra.service.auth.ras.EcmService;
import bio.terra.service.auth.ras.OidcApiService;
import bio.terra.service.auth.ras.RasDbgapPermissions;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jose.util.Base64URL;
import com.nimbusds.jwt.PlainJWT;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class EcmServiceTest {
  @Mock private EcmConfiguration ecmConfiguration;
  @Mock private RestTemplate restTemplate;
  @Mock private OidcApiService oidcApiService;
  private ObjectMapper objectMapper;
  private EcmService ecmService;
  @Mock private OidcApi oidcApi;
  @Mock private AuthenticatedUserRequest userReq;

  @Before
  public void setup() {
    objectMapper = new ObjectMapper();
    ecmService = new EcmService(ecmConfiguration, restTemplate, oidcApiService, objectMapper);
    when(oidcApiService.getOidcApi(any())).thenReturn(oidcApi);
  }

  @Test
  public void testGetRasProviderPassport() {
    String passport = "passportJwt";
    HttpClientErrorException shouldCatch = new HttpClientErrorException(HttpStatus.NOT_FOUND);
    HttpClientErrorException shouldThrow = new HttpClientErrorException(HttpStatus.UNAUTHORIZED);
    when(oidcApi.getProviderPassport(any()))
        .thenReturn(passport)
        .thenThrow(shouldCatch)
        .thenThrow(shouldThrow);

    assertThat(
        "Passport is returned", ecmService.getRasProviderPassport(userReq), equalTo(passport));
    assertThat(
        "Passport not found returns null",
        ecmService.getRasProviderPassport(userReq),
        equalTo(null));
    assertThrows(
        HttpClientErrorException.class,
        () -> ecmService.getRasProviderPassport(userReq),
        "Other exceptions are thrown");
  }

  @Test
  public void testGetRasDbgapPermissionsNoPassport() throws Exception {
    when(oidcApi.getProviderPassport(any()))
        .thenThrow(new HttpClientErrorException(HttpStatus.NOT_FOUND));
    assertThat(
        "No RAS dbGaP permissions when a user doesn't have a passport",
        ecmService.getRasDbgapPermissions(userReq),
        empty());
  }

  @Test
  public void testGetRasDbgapPermissionsNoVisaClaim() throws Exception {
    when(oidcApi.getProviderPassport(any())).thenReturn(toJwtToken("{}"));
    assertThat(
        "No RAS dbGaP permissions when a user's passport has no visa claim'",
        ecmService.getRasDbgapPermissions(userReq),
        empty());
  }

  @Test
  public void testGetRasDbgapPermissionsNoVisas() throws Exception {
    when(oidcApi.getProviderPassport(any())).thenReturn(toPassportJwt(null));
    assertThat(
        "No RAS dbGaP permissions when a user's passport has no visas",
        ecmService.getRasDbgapPermissions(userReq),
        empty());
  }

  @Test
  public void testGetRasDbgapPermissionsInvalidVisas() throws Exception {
    String invalidVisa =
        """
      {
        "sub": "EcmServiceTest",
        "ras_dbgap_permissions": "should throw InvalidDefinitionException"
      }
      """;
    when(oidcApi.getProviderPassport(any())).thenReturn(toPassportJwt(invalidVisa));

    assertThat(
        "No RAS dbGaP permissions when a user's passport has invalid visas",
        ecmService.getRasDbgapPermissions(userReq),
        empty());
  }

  @Test
  public void testGetRasDbgapPermissionsValidVisas() throws Exception {
    String validVisa =
        """
      {
        "sub": "EcmServiceTest",
        "ras_dbgap_permissions": [
          {
            "phs_id": "phs000001",
            "consent_group": "c01",
            "ignored_field": "ignored1"
          },
          {
            "phs_id": "phs000001",
            "consent_group": "c02",
            "ignored_field": "ignored1"
          },
          {
            "phs_id": "phs000002",
            "consent_group": "c02",
            "ignored_field": "ignored1"
          }
        ]
      }
      """;
    when(oidcApi.getProviderPassport(any())).thenReturn(toPassportJwt(validVisa));

    assertThat(
        "Passport visa permissions are successfully decoded and unknown properties ignored",
        ecmService.getRasDbgapPermissions(userReq),
        Matchers.contains(
            new RasDbgapPermissions("c01", "phs000001"),
            new RasDbgapPermissions("c02", "phs000001"),
            new RasDbgapPermissions("c02", "phs000002")));
  }

  /** Helper Methods for Generating Passports and JWT Tokens * */
  private String toPassportJwt(String visa) throws Exception {
    String visaJwt = (visa == null) ? "" : String.format("\"%s\"", toJwtToken(visa));
    String passportPayload =
        String.format("{\"%s\": [%s]}", EcmService.GA4GH_PASSPORT_V1_CLAIM, visaJwt);
    String returned = toJwtToken(passportPayload);
    System.out.println(returned);
    return returned;
  }

  private String toJwtToken(String payload) throws Exception {
    String header = """
        {"alg":"none","typ":"JWT"}
        """;
    return new PlainJWT(base64(header), base64(payload)).serialize();
  }

  private Base64URL base64(String toEncode) {
    byte[] bytesToEncode = toEncode.getBytes(StandardCharsets.UTF_8);
    return new Base64URL(Base64.getEncoder().withoutPadding().encodeToString(bytesToEncode));
  }
}
