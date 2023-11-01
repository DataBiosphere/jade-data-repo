package bio.terra.common.category;

/** Pact contract test category. */
public interface Pact {
  String TAG = "bio.terra.common.category.Pact";

  /**
   * The name used for Pact consumers and providers (generically known as "pacticipants") should
   * match our Helm chart name in order for our deployments to be recorded. <br>
   * Using the same name for both consumers and providers also allows for accurate and clear
   * construction of the Pact Broker's network graph.
   */
  String PACTICIPANT = "datarepo";

  /** Test application property file suffix */
  String PROFILE = "pacttest";
}
