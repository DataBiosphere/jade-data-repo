package scripts.measurementcollectionscripts;

import com.google.monitoring.v3.Aggregation;
import com.google.protobuf.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scripts.measurementcollectionscripts.baseclasses.GoogleMetric;

public class CPUCoreUsageTimeMetric extends GoogleMetric {
  private static final Logger logger = LoggerFactory.getLogger(CPUCoreUsageTimeMetric.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public CPUCoreUsageTimeMetric() {
    super();
    identifier = "kubernetes.io/container/cpu/core_usage_time";
    aggregation =
        Aggregation.newBuilder()
            .setAlignmentPeriod(Duration.newBuilder().setSeconds(60).build())
            .setPerSeriesAligner(Aggregation.Aligner.ALIGN_DELTA)
            .setCrossSeriesReducer(Aggregation.Reducer.REDUCE_MEAN)
            .build();
  }
}
