package utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AssertUtils {
  private static final Logger logger = LoggerFactory.getLogger(AssertUtils.class);

  public void assertEquals(String message, int actual, int expected) throws Exception {
    if (actual != expected) {
      logger.error(
          "Assert failed: expected {} but actual was {}; message: {}", expected, actual, message);
      throw new Exception(message);
    }
    logger.debug("Assert passed: expected & actual value = {}; message: {}", actual, message);
  }
}
