package failurescripts;

import java.util.concurrent.*;
import org.slf4j.*;
import runner.*;

public class KubernetesDeletePods extends FailureScript {
  private static final Logger logger = LoggerFactory.getLogger(KubernetesDeletePods.class);

  public void fail() throws Exception {
    TimeUnit.SECONDS.sleep(30);
    logger.info("FAILING HERE!");
  }
}
