package bio.terra.common;

import bio.terra.model.DRSAccessMethod;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.service.tabulardata.google.BigQueryProject;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.cloudresourcemanager.model.GetIamPolicyRequest;
import com.google.api.services.cloudresourcemanager.model.Policy;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static bio.terra.service.filedata.google.gcs.GcsPdao.getBlobFromGsPath;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public final class TestUtils {
    private static Logger logger = LoggerFactory.getLogger(TestUtils.class);
    private static ObjectMapper objectMapper = new ObjectMapper();

    private TestUtils() {}

    public static <T> boolean eventualExpect(
        int secInterval, int secTimeout, T expected, Callable<T> callable) throws Exception {
        LocalDateTime end = LocalDateTime.now().plus(Duration.ofSeconds(secTimeout));
        int tries = 0;
        while (LocalDateTime.now().isBefore(end)) {
            String logging = String.format("Time elapsed: %03d seconds, Tried: %03d times", secInterval * tries, tries);
            logger.info(logging);
            if (callable.call().equals(expected)) {
                return true;
            }
            TimeUnit.SECONDS.sleep(secInterval);
            tries++;
        }
        return false;
    }

    public static String validateDrsAccessMethods(List<DRSAccessMethod> accessMethods,
                                                  String token) throws IOException {
        assertThat("Two access methods", accessMethods.size(), equalTo(2));

        String gsuri = StringUtils.EMPTY;
        boolean gotGs = false;
        boolean gotHttps = false;
        for (DRSAccessMethod accessMethod : accessMethods) {
            if (accessMethod.getType() == DRSAccessMethod.TypeEnum.GS) {
                assertFalse("have not seen GS yet", gotGs);
                gsuri = accessMethod.getAccessUrl().getUrl();

                // Make sure we can actually read the file
                final Storage storage = StorageOptions.getDefaultInstance().getService();
                final String projectId = StorageOptions.getDefaultProjectId();
                getBlobFromGsPath(storage, gsuri, projectId);
                gotGs = true;
            } else if (accessMethod.getType() == DRSAccessMethod.TypeEnum.HTTPS) {
                assertFalse("have not seen HTTPS yet", gotHttps);
                // Make sure that the HTTP url is valid and accessible
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                    HttpUriRequest request = new HttpHead(accessMethod.getAccessUrl().getUrl());
                    request.setHeader("Authorization", String.format("Bearer %s", token));
                    try (
                        CloseableHttpResponse response = client.execute(request);
                    ) {
                        assertThat("Drs Https Uri is accessible",
                            response.getStatusLine().getStatusCode(),
                            equalTo(200));
                    }
                }
                gotHttps = true;
            } else {
                fail("Invalid access method");
            }
        }
        assertTrue("got both access methods", gotGs && gotHttps);
        return gsuri;
    }

    public static Map<String, List<Acl>> readDrsGCSAcls(List<DRSAccessMethod> accessMethods) {
        assertThat("Two access methods", accessMethods.size(), equalTo(2));
        for (DRSAccessMethod accessMethod : accessMethods) {
            if (accessMethod.getType() == DRSAccessMethod.TypeEnum.GS) {
                return Collections.singletonMap(
                    accessMethod.getAccessUrl().getUrl(),
                    readGCSAcls(accessMethod.getAccessUrl().getUrl())
                );
            } else {
                fail("Invalid access method");
            }
        }
        return Collections.emptyMap();
    }

    public static List<Acl> readGCSAcls(String gsPath) {
        final Storage storage = StorageOptions.getDefaultInstance().getService();
        final String projectId = StorageOptions.getDefaultProjectId();
        return getBlobFromGsPath(storage, gsPath, projectId).getAcl();
    }

    public static Policy getPolicy(final String projectId) throws GeneralSecurityException, IOException {
        HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();

        GoogleCredential credential = GoogleCredential.getApplicationDefault();
        if (credential.createScopedRequired()) {
            credential = credential.createScoped(
                Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
        }

        CloudResourceManager resourceManager = new CloudResourceManager.Builder(httpTransport, jsonFactory, credential)
            .setApplicationName("Terra Data Repo Test")
            .build();

        GetIamPolicyRequest getIamPolicyRequest = new GetIamPolicyRequest();

        return resourceManager.projects()
            .getIamPolicy(projectId, getIamPolicyRequest).execute();
    }

    public static String getHttpPathString(IamResourceType iamResourceType) {
        String httpPathString = null;
        switch (iamResourceType) {
            case DATASET:
                httpPathString = "datasets";
                break;
            case DATASNAPSHOT:
                httpPathString = "snapshots";
                break;
            default:
                httpPathString = null;
        }

        return httpPathString;
    }

    public static BigQueryProject bigQueryProjectForDatasetName(DatasetDao datasetDao,
                                                                String datasetName) throws InterruptedException {
        Dataset dataset = datasetDao.retrieveByName(datasetName);
        return BigQueryProject.get(dataset.getProjectResource().getGoogleProjectId());
    }

    private static final String selectFromBigQueryDatasetTemplate =
        "SELECT <columns> FROM `<project>.<dataset>.<table>`";

    /**
     * Execute a SELECT query on BigQuery dataset.
     * @param bigQueryPdao pass in from the calling test class
     * @param datasetDao pass in from the calling test class
     * @param dataLocationService pass in from the calling test class
     * @param datasetName the name of the Data Repo dataset
     * @param tableName the name of Data Repo table
     * @param columns a comma-separated string of the columns to select (e.g. "name", "name, fileref")
     * @return the BigQuery TableResult
     */
    public static TableResult selectFromBigQueryDataset(
        BigQueryPdao bigQueryPdao, DatasetDao datasetDao, ResourceService dataLocationService,
        String datasetName, String tableName, String columns) throws Exception {

        String bqDatasetName = bigQueryPdao.prefixName(datasetName);
        BigQueryProject bigQueryProject = bigQueryProjectForDatasetName(datasetDao, datasetName);
        String bigQueryProjectId = bigQueryProject.getProjectId();
        BigQuery bigQuery = bigQueryProject.getBigQuery();

        ST sqlTemplate = new ST(selectFromBigQueryDatasetTemplate);
        sqlTemplate.add("columns", columns);
        sqlTemplate.add("project", bigQueryProjectId);
        sqlTemplate.add("dataset", bqDatasetName);
        sqlTemplate.add("table", tableName);

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sqlTemplate.render()).build();
        return bigQuery.query(queryConfig);
    }

    public static <T> T mapFromJson(String content, Class<T> valueType) throws IOException {
        try {
            return objectMapper.readValue(content, valueType);
        } catch (IOException ex) {
            logger.error("unable to map JSON response to " +
                valueType.getName() + "JSON: " + content, ex);
            throw ex;
        }
    }

    public static String mapToJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            logger.error("unable to map value to JSON. Value is: " + value, ex);
        }
        return null;
    }

}

