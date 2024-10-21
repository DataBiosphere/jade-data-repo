package bio.terra.common.fixtures;

import java.util.UUID;

public final class Names {
  private Names() {}

  public static String randomizeName(String baseName) {
    String name = baseName + UUID.randomUUID();
    return name.replace('-', '_');
  }

  public static String randomizeNameInfix(String baseName, String infix) {
    return randomizeName(baseName + infix);
  }
}
