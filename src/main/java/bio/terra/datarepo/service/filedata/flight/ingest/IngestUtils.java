package bio.terra.datarepo.service.filedata.flight.ingest;

import bio.terra.datarepo.service.filedata.flight.FileMapKeys;
import bio.terra.datarepo.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.stairway.FlightContext;

public final class IngestUtils {
  private IngestUtils() {}

  public static GoogleBucketResource getBucketInfo(FlightContext context) {
    // The bucket has been selected for this file. In the single file load case, the info
    // is stored in the working map. In the bulk load case, the info is stored in the input
    // parameters.
    // TODO: simplify this when we remove single file load
    // TODO: This is a cut and paste from IngestFilePrimaryDataStep. Both can be removed
    //  when we get rid of single file load
    GoogleBucketResource bucketResource =
        context.getInputParameters().get(FileMapKeys.BUCKET_INFO, GoogleBucketResource.class);
    if (bucketResource == null) {
      bucketResource =
          context.getWorkingMap().get(FileMapKeys.BUCKET_INFO, GoogleBucketResource.class);
    }
    return bucketResource;
  }
}
