package bio.terra.service.dataset.flight.create;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.FlightTestUtils;
import bio.terra.common.category.Unit;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationContext;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class DatasetCreateFlightTest {
  @Mock private ApplicationContext context;
  private FlightMap inputParameters;

  @BeforeEach
  void beforeEach() {
    when(context.getBean(ApplicationConfiguration.class))
        .thenReturn(new ApplicationConfiguration());

    inputParameters = new FlightMap();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testDatasetCreateFlightAuthorizeBillingSteps(boolean isTDRBillingProfile) {
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), new DatasetRequestModel());
    inputParameters.put(JobMapKeys.TDR_BILLING_PROFILE_FALLBACK.getKeyName(), isTDRBillingProfile);
    var flight = new DatasetCreateFlight(inputParameters, context);

    if (isTDRBillingProfile) {
      assertThat(
          "Dataset creation flight locates the billing info and then has two optional steps",
          FlightTestUtils.getStepNames(flight),
          containsInRelativeOrder(
              "AuthorizeBillingProfileUseStep", "CreateDatasetInitializeProjectStep"));
    } else {
      assertThat(
          "Dataset creation flight locates the billing info and then has two optional steps",
          FlightTestUtils.getStepNames(flight),
          containsInRelativeOrder(
              "AuthorizeRawlsBillingProjectsUseStep", "CreateDatasetInitializeProjectV2Step"));
    }
  }
}
