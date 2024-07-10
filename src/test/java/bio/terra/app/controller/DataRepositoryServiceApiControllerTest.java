package bio.terra.app.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.linkTo;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.methodOn;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.options;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.model.DRSAccessURL;
import bio.terra.model.DRSAuthorizations;
import bio.terra.model.DRSObject;
import bio.terra.model.DRSPassportRequestModel;
import bio.terra.service.filedata.DrsService;
import bio.terra.service.filedata.exception.DrsObjectNotFoundException;
import java.net.URI;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;

@ActiveProfiles({"google", "unittest"})
@ContextConfiguration(classes = {DataRepositoryServiceApiController.class})
@Tag(Unit.TAG)
@WebMvcTest
@MockBean(ApplicationConfiguration.class)
class DataRepositoryServiceApiControllerTest {

  // Test fixtures
  private static final String DRS_ID = "foo";
  private static final String DRS_ACCESS_ID = "bar";
  private static final DRSObject DRS_OBJECT = new DRSObject().id(DRS_ID);
  private static final String DRS_ACCESS_URL = "http://foo.bar/baz";
  private static final DRSAccessURL DRS_ACCESS_URL_OBJECT = new DRSAccessURL().url(DRS_ACCESS_URL);
  private static final DRSPassportRequestModel PASSPORT = new DRSPassportRequestModel();
  private static final String PASSPORT_ISSUER = "baz";
  private static final DRSAuthorizations PASSPORT_AUTHORIZATIONS =
      new DRSAuthorizations().addPassportAuthIssuersItem(PASSPORT_ISSUER);

  @Autowired private MockMvc mvc;

  @MockBean private DrsService drsService;
  @MockBean private AuthenticatedUserRequestFactory authenticatedUserRequestFactory;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  @BeforeEach
  void setUp() {
    when(authenticatedUserRequestFactory.from(any())).thenReturn(TEST_USER);
  }

  private static <T> URI createUri(ResponseEntity<T> object) {
    return linkTo(object).toUri();
  }

  private static DataRepositoryServiceApiController getApi() {
    return methodOn(DataRepositoryServiceApiController.class);
  }

  private static URI createGetUri() {
    return createUri(getApi().getObject(DRS_ID, false));
  }

  private static URI createAccessUri() {
    return createUri(getApi().getAccessURL(DRS_ID, DRS_ACCESS_ID, null));
  }

  @Test
  void testUnknownDrsIdWithGetFlow() throws Exception {
    when(drsService.lookupObjectByDrsId(TEST_USER, DRS_ID, false))
        .thenThrow(DrsObjectNotFoundException.class);
    mvc.perform(get(createGetUri())).andExpect(status().isNotFound());

    when(drsService.getAccessUrlForObjectId(TEST_USER, DRS_ID, DRS_ACCESS_ID, null))
        .thenThrow(DrsObjectNotFoundException.class);
    mvc.perform(get(createAccessUri())).andExpect(status().isNotFound());
  }

  @Test
  void testKnownDrsIdWithGetFlow() throws Exception {
    when(drsService.lookupObjectByDrsId(TEST_USER, DRS_ID, false)).thenReturn(DRS_OBJECT);
    mvc.perform(get(createGetUri()))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.id").value(DRS_ID));

    when(drsService.getAccessUrlForObjectId(TEST_USER, DRS_ID, DRS_ACCESS_ID, null))
        .thenReturn(DRS_ACCESS_URL_OBJECT);
    mvc.perform(get(createAccessUri()))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.url").value(DRS_ACCESS_URL));
  }

  private static URI createPostObjectUri() {
    return createUri(getApi().postObject(DRS_ID, PASSPORT));
  }

  private static URI createPostAccessUri() {
    return createUri(getApi().postAccessURL(DRS_ID, DRS_ACCESS_ID, PASSPORT, null));
  }

  @Test
  void testUnknownDrsIdWithPostFlow() throws Exception {
    when(drsService.lookupObjectByDrsIdPassport(DRS_ID, PASSPORT))
        .thenThrow(DrsObjectNotFoundException.class);
    mvc.perform(
        post(createPostObjectUri())
            .contentType(MediaType.APPLICATION_JSON)
            .content(TestUtils.mapToJson(PASSPORT)));

    when(drsService.postAccessUrlForObjectId(DRS_ID, DRS_ACCESS_ID, PASSPORT, null))
        .thenThrow(DrsObjectNotFoundException.class);
    mvc.perform(
            post(createPostAccessUri())
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestUtils.mapToJson(PASSPORT)))
        .andExpect(status().isNotFound());
  }

  @Test
  void testKnownDrsIdWithPostFlow() throws Exception {
    when(drsService.lookupObjectByDrsIdPassport(DRS_ID, PASSPORT)).thenReturn(DRS_OBJECT);
    mvc.perform(
            post(createPostObjectUri())
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestUtils.mapToJson(PASSPORT)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.id").value(DRS_ID));

    when(drsService.postAccessUrlForObjectId(DRS_ID, DRS_ACCESS_ID, PASSPORT, null))
        .thenReturn(DRS_ACCESS_URL_OBJECT);
    mvc.perform(
            post(createPostAccessUri())
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestUtils.mapToJson(PASSPORT)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.url").value(DRS_ACCESS_URL));
  }

  @Test
  void testUnknownDrsIdWithOptionsFlow() throws Exception {
    when(drsService.lookupAuthorizationsByDrsId(DRS_ID))
        .thenThrow(DrsObjectNotFoundException.class);
    mvc.perform(options(createGetUri())).andExpect(status().isNotFound());
  }

  @Test
  void testKnownDrsIdWithOptionsFlow() throws Exception {
    when(drsService.lookupAuthorizationsByDrsId(DRS_ID)).thenReturn(PASSPORT_AUTHORIZATIONS);
    mvc.perform(options(createGetUri()))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.passport_auth_issuers[0]").value(PASSPORT_ISSUER));
  }
}
