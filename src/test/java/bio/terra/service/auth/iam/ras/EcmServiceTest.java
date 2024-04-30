package bio.terra.service.auth.iam.ras;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.EcmConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

@Tag(Unit.TAG)
@ExtendWith(MockitoExtension.class)
class EcmServiceTest {
  private EcmService ecmService;
  @Mock private OidcApi oidcApi;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  @BeforeEach
  void setup() {
    ObjectMapper objectMapper = new ObjectMapper();
    OidcApiService oidcApiService = mock(OidcApiService.class);
    ecmService =
        new EcmService(
            mock(EcmConfiguration.class), mock(RestTemplate.class), oidcApiService, objectMapper);
    when(oidcApiService.getOidcApi(TEST_USER)).thenReturn(oidcApi);
  }

  @Test
  void testGetRasProviderPassport() {
    String passport = "passportJwt";
    HttpClientErrorException shouldCatch = new HttpClientErrorException(HttpStatus.NOT_FOUND);
    HttpClientErrorException shouldThrow = new HttpClientErrorException(HttpStatus.UNAUTHORIZED);
    when(oidcApi.getProviderPassport(any()))
        .thenReturn(passport)
        .thenThrow(shouldCatch)
        .thenThrow(shouldThrow);

    assertThat(
        "Passport is returned", ecmService.getRasProviderPassport(TEST_USER), equalTo(passport));
    assertThat(
        "Passport not found returns null",
        ecmService.getRasProviderPassport(TEST_USER),
        equalTo(null));
    assertThrows(
        HttpClientErrorException.class,
        () -> ecmService.getRasProviderPassport(TEST_USER),
        "Other exceptions are thrown");
  }

  @Test
  void testGetRasDbgapPermissionsNoPassport() throws Exception {
    when(oidcApi.getProviderPassport(any()))
        .thenThrow(new HttpClientErrorException(HttpStatus.NOT_FOUND));
    assertThat(
        "No RAS dbGaP permissions when a user doesn't have a passport",
        ecmService.getRasDbgapPermissions(TEST_USER),
        empty());
  }

  @Test
  void testGetRasDbgapPermissionsNoVisaClaim() throws Exception {
    when(oidcApi.getProviderPassport(any())).thenReturn(toJwtToken("{}"));
    assertThat(
        "No RAS dbGaP permissions when a user's passport has no visa claim'",
        ecmService.getRasDbgapPermissions(TEST_USER),
        empty());
  }

  @Test
  void testGetRasDbgapPermissionsNoVisas() throws Exception {
    when(oidcApi.getProviderPassport(any())).thenReturn(toPassportJwt(null));
    assertThat(
        "No RAS dbGaP permissions when a user's passport has no visas",
        ecmService.getRasDbgapPermissions(TEST_USER),
        empty());
  }

  @Test
  void testGetRasDbgapPermissionsInvalidVisas() throws Exception {
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
        ecmService.getRasDbgapPermissions(TEST_USER),
        empty());
  }

  @Test
  void testGetRasDbgapPermissionsValidVisas() throws Exception {
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
        ecmService.getRasDbgapPermissions(TEST_USER),
        Matchers.contains(
            new RasDbgapPermissions("c01", "phs000001"),
            new RasDbgapPermissions("c02", "phs000001"),
            new RasDbgapPermissions("c02", "phs000002")));
  }

  /** Helper Methods for Generating Passports and JWT Tokens * */
  private String toPassportJwt(String visa) throws Exception {
    String visaJwt = (visa == null) ? "" : String.format("\"%s\"", toJwtToken(visa));
    String passportPayload =
        """
            {"%s": [%s]}""".formatted(EcmService.GA4GH_PASSPORT_V1_CLAIM, visaJwt);
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
