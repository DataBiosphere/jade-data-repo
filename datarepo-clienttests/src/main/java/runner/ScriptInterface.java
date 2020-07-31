package runner;

import bio.terra.datarepo.client.*;

// Interface created primarily to allow both TestScripts and FailureScripts to be added to the same
// user journey thread pool
public interface ScriptInterface {

  void userJourney(ApiClient apiClient) throws Exception;
}
