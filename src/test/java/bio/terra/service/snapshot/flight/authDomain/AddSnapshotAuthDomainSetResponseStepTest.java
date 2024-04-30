package bio.terra.service.snapshot.flight.authDomain;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.AddAuthDomainResponseModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class AddSnapshotAuthDomainSetResponseStepTest {

  @Mock private IamService iamService;
  @Mock private FlightContext flightContext;
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private static final UUID SNAPSHOT_ID = UUID.randomUUID();
  private static final List<String> userGroups = List.of("group1", "group2");

  @Test
  void testDoAndUndoStepSucceeds() {
    AddSnapshotAuthDomainSetResponseStep step =
        new AddSnapshotAuthDomainSetResponseStep(iamService, TEST_USER, SNAPSHOT_ID);
    when(iamService.retrieveAuthDomain(TEST_USER, IamResourceType.DATASNAPSHOT, SNAPSHOT_ID))
        .thenReturn(userGroups);
    when(flightContext.getWorkingMap()).thenReturn(new FlightMap());

    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(iamService).retrieveAuthDomain(TEST_USER, IamResourceType.DATASNAPSHOT, SNAPSHOT_ID);

    FlightMap workingMap = flightContext.getWorkingMap();
    assertEquals(
        HttpStatus.OK, workingMap.get(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.class));
    assertTrue(
        workingMap
            .get(JobMapKeys.RESPONSE.getKeyName(), AddAuthDomainResponseModel.class)
            .getAuthDomain()
            .containsAll(userGroups));
  }
}
