package bio.terra.service.common;

import static bio.terra.service.common.CommonFlightUtils.getFlightInformationOfInterest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.Direction;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.ProgressMeter;
import bio.terra.stairway.Stairway;
import bio.terra.stairway.StepResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class CommonFlightUtilsTest {
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  @Test
  void journalMapShouldRemoveUserInfo() throws JsonProcessingException {
    FlightMap flightMap = new FlightMap();
    flightMap.put(JobMapKeys.AUTH_USER_INFO.getKeyName(), TEST_USER);
    flightMap.put(JobMapKeys.DESCRIPTION.getKeyName(), null);
    flightMap.put(JobMapKeys.REQUEST.getKeyName(), "A request string");
    FlightContext flightContext =
        new FlightContext() {

          @Override
          public Object getApplicationContext() {
            return null;
          }

          @Override
          public String getFlightId() {
            return null;
          }

          @Override
          public String getFlightClassName() {
            return null;
          }

          @Override
          public FlightMap getInputParameters() {
            return flightMap;
          }

          @Override
          public FlightMap getWorkingMap() {
            return null;
          }

          @Override
          public int getStepIndex() {
            return 0;
          }

          @Override
          public FlightStatus getFlightStatus() {
            return null;
          }

          @Override
          public boolean isRerun() {
            return false;
          }

          @Override
          public Direction getDirection() {
            return null;
          }

          @Override
          public StepResult getResult() {
            return null;
          }

          @Override
          public Stairway getStairway() {
            return null;
          }

          @Override
          public List<String> getStepClassNames() {
            return null;
          }

          @Override
          public String getStepClassName() {
            return null;
          }

          @Override
          public String prettyStepState() {
            return null;
          }

          @Override
          public String flightDesc() {
            return null;
          }

          @Override
          public ProgressMeter getProgressMeter(String name) {
            return null;
          }

          @Override
          public void setProgressMeter(String name, long v1, long v2) throws InterruptedException {}
        };

    assertThat(
        "map should have an auth_user_info  entry.",
        flightContext.getInputParameters().containsKey(JobMapKeys.AUTH_USER_INFO.getKeyName()),
        equalTo(true));
    assertThat(
        "map should not have an entry for auth user.",
        getFlightInformationOfInterest(flightContext)
            .containsKey(JobMapKeys.AUTH_USER_INFO.getKeyName()),
        equalTo(false));
    assertThat(
        "flight map entry for null value returns null",
        getFlightInformationOfInterest(flightContext).get(JobMapKeys.DESCRIPTION.getKeyName()),
        equalTo(null));
    assertThat(
        "map should contain a key with the JSON hint at the end.",
        getFlightInformationOfInterest(flightContext)
            .get(JobMapKeys.REQUEST.getKeyName() + ".json"),
        equalTo("\"A request string\""));
  }
}
