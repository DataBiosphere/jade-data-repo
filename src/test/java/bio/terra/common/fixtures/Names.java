package bio.terra.common.fixtures;

import java.util.UUID;
import org.apache.commons.lang3.StringUtils;

public final class Names {
  private Names() {}

  public static String randomizeName(String baseName) {
    String name = baseName + UUID.randomUUID();
    return StringUtils.replaceChars(name, '-', '_');
  }

  public static String randomizeNameInfix(String baseName, String infix) {
    return randomizeName(baseName + infix);
  }
}
