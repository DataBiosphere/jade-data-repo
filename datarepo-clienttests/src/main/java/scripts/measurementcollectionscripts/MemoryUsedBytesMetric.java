package scripts.measurementcollectionscripts;

import com.google.monitoring.v3.Aggregation;
import com.google.protobuf.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scripts.measurementcollectionscripts.baseclasses.GoogleMetric;

public class MemoryUsedBytesMetric extends GoogleMetric {
  private static final Logger logger = LoggerFactory.getLogger(MemoryUsedBytesMetric.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public MemoryUsedBytesMetric() {
    super();
    identifier = "kubernetes.io/container/memory/used_bytes";
    aggregation =
        Aggregation.newBuilder()
            .setAlignmentPeriod(Duration.newBuilder().setSeconds(60).build())
            .setPerSeriesAligner(Aggregation.Aligner.ALIGN_MEAN)
            .setCrossSeriesReducer(Aggregation.Reducer.REDUCE_MEAN)
            .build();
  }
}
