package bio.terra.service.auth.iam.ras;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import bio.terra.app.configuration.EcmConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.externalcreds.api.OidcApi;
import bio.terra.service.auth.ras.EcmService;
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
  @Mock private OidcApi oidcApi;
  private ObjectMapper objectMapper;
  private JsonLoader jsonLoader;
  private EcmService ecmService;
  private AuthenticatedUserRequest authenticatedUserRequest;

  @Before
  public void setup() {
    objectMapper = new ObjectMapper();
    jsonLoader = new JsonLoader(objectMapper);
    ecmService = spy(new EcmService(ecmConfiguration, restTemplate, objectMapper));
    authenticatedUserRequest =
        AuthenticatedUserRequest.builder()
            .setSubjectId("DatasetUnit")
            .setEmail("dataset@unit.com")
            .setToken("token")
            .build();
    doAnswer(a -> oidcApi).when(ecmService).getOidcApi(authenticatedUserRequest);
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
        "Passport is returned",
        ecmService.getRasProviderPassport(authenticatedUserRequest),
        equalTo(passport));
    assertThat(
        "Passport not found returns null",
        ecmService.getRasProviderPassport(authenticatedUserRequest),
        equalTo(null));
    assertThrows(
        HttpClientErrorException.class,
        () -> ecmService.getRasProviderPassport(authenticatedUserRequest),
        "Other exceptions are thrown");
  }

  @Test
  public void testGetRasDbgapPermissions() throws Exception {
    // Note: many of the "unhappy paths" in this test should not occur if ECM only returns valid
    // passports and visas as indicated by their Swagger documentation.
    // Testing them anyway to verify our own behavior.
    doAnswer(a -> null).when(ecmService).getRasProviderPassport(authenticatedUserRequest);
    assertThat(
        "No RAS dbGaP permissions when a user doesn't have a passport",
        ecmService.getRasDbgapPermissions(authenticatedUserRequest),
        empty());

    doAnswer(a -> toJwtToken("{}"))
        .when(ecmService)
        .getRasProviderPassport(authenticatedUserRequest);
    assertThat(
        "No RAS dbGaP permissions when a user's passport has no visa claim'",
        ecmService.getRasDbgapPermissions(authenticatedUserRequest),
        empty());

    String passportPayloadNoVisas =
        String.format("{\"%s\":[]}", EcmService.GA4GH_PASSPORT_V1_CLAIM);
    doAnswer(a -> toJwtToken(passportPayloadNoVisas))
        .when(ecmService)
        .getRasProviderPassport(authenticatedUserRequest);
    assertThat(
        "No RAS dbGaP permissions when a user's passport has no visas",
        ecmService.getRasDbgapPermissions(authenticatedUserRequest),
        empty());

    String invalidVisaJwt = toJwtToken(loadJson("ga4gh_passport_v1_invalid.json"));
    String invalidPassportPayload =
        String.format("{\"%s\":[\"%s\"]}", EcmService.GA4GH_PASSPORT_V1_CLAIM, invalidVisaJwt);
    String invalidPassportJwt = toJwtToken(invalidPassportPayload);
    doAnswer(a -> invalidPassportJwt)
        .when(ecmService)
        .getRasProviderPassport(authenticatedUserRequest);
    assertThat(
        "No RAS dbGaP permissions when a user's passport has invalid visas",
        ecmService.getRasDbgapPermissions(authenticatedUserRequest),
        empty());

    String visaJwt = toJwtToken(loadJson("ga4gh_passport_v1_valid.json"));
    String passportPayload =
        String.format("{\"%s\":[\"%s\"]}", EcmService.GA4GH_PASSPORT_V1_CLAIM, visaJwt);
    String passportJwt = toJwtToken(passportPayload);
    doAnswer(a -> passportJwt).when(ecmService).getRasProviderPassport(authenticatedUserRequest);
    assertThat(
        "Passport visa permissions are successfully decoded and unknown properties ignored",
        ecmService.getRasDbgapPermissions(authenticatedUserRequest),
        Matchers.contains(
            new RasDbgapPermissions("c01", "phs000001"),
            new RasDbgapPermissions("c02", "phs000001"),
            new RasDbgapPermissions("c02", "phs000002")));
  }

  /** Helper Methods for Generating JWT Tokens * */
  private String loadJson(String filename) throws Exception {
    return jsonLoader.loadJson(String.format("./service/auth/iam/ras/%s", filename));
  }

  private String toJwtToken(String payload) throws Exception {
    String header = "{\"alg\":\"none\",\"typ\":\"JWT\"}";
    return new PlainJWT(base64(header), base64(payload)).serialize();
  }

  private Base64URL base64(String toEncode) {
    byte[] bytesToEncode = toEncode.getBytes(StandardCharsets.UTF_8);
    return new Base64URL(Base64.getEncoder().withoutPadding().encodeToString(bytesToEncode));
  }
}
