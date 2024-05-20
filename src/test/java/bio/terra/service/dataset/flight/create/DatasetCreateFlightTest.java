package bio.terra.service.dataset.flight.create;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.StepUtils;
import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;

@Tag(Unit.TAG)
class DatasetCreateFlightTest {
  @Test
  void testDataFlow() {
    FlightMap inputParameters = new FlightMap();
    ApplicationContext mockContext = mock(ApplicationContext.class);
    var configuration = new ApplicationConfiguration();
    configuration.setMaxStairwayThreads(1);
    when(mockContext.getBean(ApplicationConfiguration.class)).thenReturn(configuration);
    inputParameters.put(
        JobMapKeys.REQUEST.getKeyName(),
        new DatasetRequestModel().cloudPlatform(CloudPlatform.GCP));
    Flight flight = new DatasetCreateFlight(inputParameters, mockContext);
    StepUtils.generateFlowAnalysisReport(flight).forEach(System.out::println);
  }
}
