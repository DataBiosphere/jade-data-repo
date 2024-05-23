package bio.terra.app.controller;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.common.TestUtils;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.model.EnumerateSnapshotAccessRequest;
import bio.terra.model.SnapshotAccessRequest;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import bio.terra.service.snapshotbuilder.SnapshotBuilderTestData;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;

@ActiveProfiles({"google", "unittest"})
@ContextConfiguration(
    classes = {SnapshotAccessRequestApiController.class, GlobalExceptionHandler.class})
@Tag("bio.terra.common.category.Unit")
@WebMvcTest
class SnapshotAccessRequestApiControllerTest {
  @Autowired private MockMvc mvc;
  @MockBean private AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  @MockBean private SnapshotBuilderService snapshotBuilderService;
  @MockBean private IamService iamService;

  private static final String ENDPOINT = "/api/repository/v1/snapshotAccessRequests";

  private static final String REJECT_ENDPOINT = ENDPOINT + "/{id}/reject";
  private static final String APPROVE_ENDPOINT = ENDPOINT + "/{id}/approve";

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  private static final UUID SNAPSHOT_ID = UUID.randomUUID();

  @BeforeEach
  void setUp() {
    when(authenticatedUserRequestFactory.from(any())).thenReturn(TEST_USER);
  }

  @Test
  void testCreateSnapshotRequest() throws Exception {
    SnapshotAccessRequest request =
        SnapshotBuilderTestData.createSnapshotAccessRequest(SNAPSHOT_ID);

    SnapshotAccessRequestResponse expectedResponse =
        SnapshotBuilderTestData.createSnapshotAccessRequestResponse(SNAPSHOT_ID);
    when(snapshotBuilderService.createRequest(any(), eq(request))).thenReturn(expectedResponse);
    String actualJson =
        mvc.perform(
                post(ENDPOINT)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(request)))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();
    SnapshotAccessRequestResponse actual =
        TestUtils.mapFromJson(actualJson, SnapshotAccessRequestResponse.class);
    assertThat("The method returned the expected response", actual, equalTo(expectedResponse));
    verify(iamService)
        .verifyAuthorization(
            TEST_USER,
            IamResourceType.DATASNAPSHOT,
            SNAPSHOT_ID.toString(),
            IamAction.CREATE_SNAPSHOT_REQUEST);
  }

  @Test
  void testEnumerateSnapshotRequests() throws Exception {
    var expectedResponseItem =
        SnapshotBuilderTestData.createSnapshotAccessRequestResponse(SNAPSHOT_ID);
    var secondExpectedResponseItem =
        SnapshotBuilderTestData.createSnapshotAccessRequestResponse(SNAPSHOT_ID);
    var expectedResponse = new EnumerateSnapshotAccessRequest();
    Map<UUID, Set<IamRole>> authResponse =
        Map.of(
            expectedResponseItem.getId(), Set.of(), secondExpectedResponseItem.getId(), Set.of());
    expectedResponse.items(List.of(expectedResponseItem, secondExpectedResponseItem));
    when(iamService.listAuthorizedResources(TEST_USER, IamResourceType.SNAPSHOT_BUILDER_REQUEST))
        .thenReturn(authResponse);
    when(snapshotBuilderService.enumerateRequests(authResponse.keySet()))
        .thenReturn(expectedResponse);
    String actualJson =
        mvc.perform(get(ENDPOINT))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();
    EnumerateSnapshotAccessRequest actual =
        TestUtils.mapFromJson(actualJson, EnumerateSnapshotAccessRequest.class);
    assertThat("The method returned the expected response", actual, equalTo(expectedResponse));
  }

  @Test
  void testApproveAndRejectSnapshotRequest() throws Exception {
    UUID id = UUID.randomUUID();
    SnapshotAccessRequestResponse response = new SnapshotAccessRequestResponse().id(id);
    when(snapshotBuilderService.rejectRequest(id)).thenReturn(response);
    testUpdateStatus(id, response, REJECT_ENDPOINT);
  }

  @Test
  void testApproveSnapshotRequest() throws Exception {
    UUID id = UUID.randomUUID();
    SnapshotAccessRequestResponse response = new SnapshotAccessRequestResponse().id(id);
    when(snapshotBuilderService.approveRequest(id)).thenReturn(response);
    testUpdateStatus(id, response, APPROVE_ENDPOINT);
  }

  private void testUpdateStatus(UUID id, SnapshotAccessRequestResponse response, String endpoint)
      throws Exception {
    String actualJson =
        mvc.perform(put(endpoint, id))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();
    SnapshotAccessRequestResponse actual =
        TestUtils.mapFromJson(actualJson, SnapshotAccessRequestResponse.class);
    assertThat("The updated response is returned", actual, equalTo(response));
    verify(iamService)
        .verifyAuthorization(
            TEST_USER, IamResourceType.SNAPSHOT_BUILDER_REQUEST, id.toString(), IamAction.APPROVE);
  }
}
