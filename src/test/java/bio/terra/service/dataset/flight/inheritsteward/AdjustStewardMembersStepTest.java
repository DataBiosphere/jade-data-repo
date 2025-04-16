package bio.terra.service.dataset.flight.inheritsteward;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class AdjustStewardMembersStepTest {

  @Mock private IamService iamService;
  @Mock private FlightContext flightContext;
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  interface DoOrUndo {
    StepResult apply(FlightContext t) throws Exception;
  }

  private void verifyAdjustMembers(DoOrUndo doOrUndo, boolean inheritSteward) throws Exception {
    var snapshots =
        List.of(new Snapshot().id(UUID.randomUUID()), new Snapshot().id(UUID.randomUUID()));
    List<String> datasetPolicyEmails = new ArrayList<>(List.of("custodianEmail", "stewardEmail"));
    FlightMap workingMap = new FlightMap();
    workingMap.put(
        DatasetWorkingMapKeys.SNAPSHOT_IDS,
        snapshots.stream().map(Snapshot::getId).collect(Collectors.toList()));
    FlightMap inputParameters = new FlightMap();
    inputParameters.put(JobMapKeys.DATASET_POLICY_USERS.getKeyName(), datasetPolicyEmails);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    when(flightContext.getInputParameters()).thenReturn(inputParameters);
    assertThat(doOrUndo.apply(flightContext), is(StepResult.getStepResultSuccess()));
    for (var snapshot : snapshots) {
      if (inheritSteward) {
        datasetPolicyEmails.forEach(
            email ->
                verify(iamService)
                    .deletePolicyMember(
                        TEST_USER,
                        IamResourceType.DATASNAPSHOT,
                        snapshot.getId(),
                        IamRole.STEWARD,
                        email));
      } else {
        datasetPolicyEmails.forEach(
            email ->
                verify(iamService)
                    .addPolicyMember(
                        TEST_USER,
                        IamResourceType.DATASNAPSHOT,
                        snapshot.getId(),
                        IamRole.STEWARD,
                        email));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void doStep(boolean inheritSteward) throws Exception {
    AdjustStewardMembersStep step =
        new AdjustStewardMembersStep(TEST_USER, iamService, inheritSteward);
    verifyAdjustMembers(step::doStep, inheritSteward);
    verifyAdjustMembers(step::undoStep, !inheritSteward);
  }
}
