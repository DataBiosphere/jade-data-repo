package bio.terra.service.resourcemanagement.flight;

import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.google.GoogleBucketService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class BucketAutoclassUpdateFlight extends Flight {
  public BucketAutoclassUpdateFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    ApplicationContext appContext = (ApplicationContext) applicationContext;
    GoogleBucketService googleBucketService = appContext.getBean(GoogleBucketService.class);
    String bucketName = inputParameters.get(JobMapKeys.BUCKET_NAME.getKeyName(), String.class);

    addStep(new RecordBucketAutoclassStep(googleBucketService, bucketName));
    addStep(new UpdateBucketAutoclassStep(googleBucketService));
    addStep(new UpdateDatabaseAutoclassStep(googleBucketService));
  }
}
