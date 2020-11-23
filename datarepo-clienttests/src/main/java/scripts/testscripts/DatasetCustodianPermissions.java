package scripts.testscripts;

import static org.hamcrest.MatcherAssert.assertThat;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.client.ApiException;
import bio.terra.datarepo.model.DatasetModel;
import bio.terra.datarepo.model.PolicyMemberRequest;
import bio.terra.datarepo.model.PolicyResponse;
import bio.terra.datarepo.model.TableModel;
import bio.terra.testrunner.common.utils.BigQueryUtils;
import bio.terra.testrunner.runner.config.TestUserSpecification;
import com.google.api.client.http.HttpStatusCodes;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.TableResult;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scripts.testscripts.baseclasses.SimpleDataset;
import scripts.utils.DataRepoUtils;
import scripts.utils.SAMUtils;

public class DatasetCustodianPermissions extends SimpleDataset {
  private static final Logger logger = LoggerFactory.getLogger(DatasetCustodianPermissions.class);

  private DatasetModel datasetModel;

  /** Public constructor so that this class can be instantiated via reflection. */
  public DatasetCustodianPermissions() {
    super();
  }

  public void setup(List<TestUserSpecification> testUsers) throws Exception {
    // create the profile and dataset
    super.setup(testUsers);

    // fetch the full dataset model, which has the data project property
    ApiClient datasetCreatorClient = DataRepoUtils.getClientForTestUser(datasetCreator, server);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);
    datasetModel = repositoryApi.retrieveDataset(datasetSummaryModel.getId());
  }

  public void userJourney(TestUserSpecification testUser) throws Exception {
    // check with SAM if the user is a steward
    org.broadinstitute.dsde.workbench.client.sam.ApiClient samClient =
        SAMUtils.getClientForTestUser(testUser, server);
    boolean isSteward = SAMUtils.isDataRepoSteward(samClient, server.samResourceIdForDatarepo);
    logger.info("testUser {} isSteward = {}", testUser.name, isSteward);

    // try #1 to retrieve the dataset
    ApiClient datarepoClient = DataRepoUtils.getClientForTestUser(testUser, server);
    RepositoryApi repositoryApi = new RepositoryApi(datarepoClient);
    boolean retrieveDatasetIsUnauthorized =
        retrieveDatasetIsUnauthorized(repositoryApi, datasetSummaryModel.getId());

    // try #1 to select from the bigquery table
    BigQuery bigQueryClient =
        BigQueryUtils.getClientForTestUser(testUser, datasetModel.getDataProject());
    // there is only one table in SimpleDataset to select from
    TableModel tableModel = datasetModel.getSchema().getTables().get(0);
    String selectCountQuery =
        "SELECT COUNT(1) AS NUM_ROWS FROM "
            + DataRepoUtils.getBigQueryDatasetTableName(datasetModel, tableModel);
    boolean selectFromBQDatasetIsUnauthorized =
        selectFromBQDatasetIsUnauthorized(bigQueryClient, selectCountQuery);

    // stewards should have access, non-stewards should not
    if (isSteward) {
      assertThat(
          "Test user " + testUser.name + " is a steward and can retrieve the dataset",
          !retrieveDatasetIsUnauthorized);
      assertThat(
          "Test user " + testUser.name + " is a steward and can select from the BigQuery dataset",
          !selectFromBQDatasetIsUnauthorized);

      // stewards already have permissions, so only check custodian/reader permissions for
      // non-stewards
      return;
    }
    assertThat(
        "Test user " + testUser.name + " is not a steward and cannot retrieve the dataset",
        retrieveDatasetIsUnauthorized);
    assertThat(
        "Test user "
            + testUser.name
            + " is not a steward and cannot select from the BigQuery dataset",
        selectFromBQDatasetIsUnauthorized);

    // add test user as a custodian
    ApiClient datasetCreatorDatarepoClient =
        DataRepoUtils.getClientForTestUser(datasetCreator, server);
    RepositoryApi datasetCreatorRepositoryApi = new RepositoryApi(datasetCreatorDatarepoClient);
    PolicyResponse policyResponse =
        datasetCreatorRepositoryApi.addDatasetPolicyMember(
            datasetSummaryModel.getId(),
            "custodian",
            new PolicyMemberRequest().email(testUser.userEmail));
    logger.debug("policyresponse {}", policyResponse);

    // try #2 to retrieve the dataset
    retrieveDatasetIsUnauthorized =
        retrieveDatasetIsUnauthorized(repositoryApi, datasetSummaryModel.getId());
    assertThat(
        "Test user " + testUser.name + " is a custodian and can retrieve the dataset",
        !retrieveDatasetIsUnauthorized);

    // TODO: should custodians be able to query the BigQuery table for a dataset?
    // they don't seem to be able to, but I don't know if that's intended behavior
    // regardless, put a check for the intended behavior here
  }

  /**
   * Check if retrieving a dataset returns an unauthorized error.
   *
   * @param repositoryApi the api object to query
   * @return true if the endpoint returns an unauthorized error, false if it does not
   */
  private static boolean retrieveDatasetIsUnauthorized(
      RepositoryApi repositoryApi, String datasetId) throws ApiException {
    boolean caughtAccessException = false;
    try {
      DatasetModel datasetModel = repositoryApi.retrieveDataset(datasetId);
      logger.debug(
          "Successfully retrieved dataset: name = {}, data project = {}",
          datasetModel.getName(),
          datasetModel.getDataProject());
    } catch (ApiException drApiEx) {
      logger.debug("caught exception retrieving dataset code = {}", drApiEx.getCode(), drApiEx);
      if (drApiEx.getCode() == HttpStatusCodes.STATUS_CODE_UNAUTHORIZED) {
        caughtAccessException = true;
      } else {
        throw drApiEx;
      }
    }
    return caughtAccessException;
  }

  /**
   * Check if executing a BigQuery query returns an unauthorized error.
   *
   * @param bigQueryClient the BigQuery client object to use
   * @return true if the endpoint returns an unauthorized error, false if it does not
   */
  private static boolean selectFromBQDatasetIsUnauthorized(BigQuery bigQueryClient, String query)
      throws Exception {
    boolean caughtAccessException = false;
    try {
      TableResult queryResult = BigQueryUtils.queryBigQuery(bigQueryClient, query);
      logger.info(
          "Successfully selected from BigQuery dataset: {} , {}",
          query,
          queryResult.getValues().iterator().next().get("NUM_ROWS"));
    } catch (BigQueryException bqEx) {
      logger.debug("caught exception selecting from BigQuery dataset = {}", bqEx.getCode(), bqEx);
      if (bqEx.getCode() == HttpStatusCodes.STATUS_CODE_FORBIDDEN) {
        caughtAccessException = true;
      } else {
        throw bqEx;
      }
    }
    return caughtAccessException;
  }
}
