package bio.terra.datarepo.service.configuration;

/** FaultFunction provides a lambda target for a function with no parameters and no return value. */
@FunctionalInterface
public interface FaultFunction {
  void apply() throws Exception;
}
