package runner.config;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class TestConfiguration implements SpecificationInterface {
    public String name;
    public String serverSpecificationFile;
    public List<String> testUserFiles;

    public ServerSpecification server;
    public List<TestScriptSpecification> testScripts;
    public List<TestUserSpecification> testUsers = new ArrayList<>();

    public static final String resourceDirectory = "configs";

    public TestConfiguration() { }

    public static TestConfiguration fromJSONFile(String resourceFileName) throws Exception {
        // use Jackson to map the stream contents to a TestConfiguration object
        ObjectMapper objectMapper = new ObjectMapper();

        // read in the test config file
        InputStream inputStream = getJSONFileHandle(resourceDirectory + "/" + resourceFileName);
        TestConfiguration testConfig = objectMapper.readValue(inputStream, TestConfiguration.class);

        // read in the server file
        inputStream = getJSONFileHandle(ServerSpecification.resourceDirectory + "/" + testConfig.serverSpecificationFile);
        testConfig.server = objectMapper.readValue(inputStream, ServerSpecification.class);

        // read in the test user files and the nested service account files
        for (String testUserFile : testConfig.testUserFiles) {
            inputStream = getJSONFileHandle(TestUserSpecification.resourceDirectory + "/" + testUserFile);
            TestUserSpecification testUser = objectMapper.readValue(inputStream, TestUserSpecification.class);

            inputStream = getJSONFileHandle(ServiceAccountSpecification.resourceDirectory + "/" + testUser.delegatorServiceAccountFile);
            testUser.delegatorServiceAccount = objectMapper.readValue(inputStream, ServiceAccountSpecification.class);
            testConfig.testUsers.add(testUser);
        }

        return testConfig;
    }

    /**
     * Build a stream handle to a JSON resource file.
     * @throws FileNotFoundException
     */
    private static InputStream getJSONFileHandle(String resourceFilePath) throws FileNotFoundException {
        InputStream inputStream = TestConfiguration.class.getClassLoader().getResourceAsStream(resourceFilePath);
        if (inputStream == null) {
            throw new FileNotFoundException("Resource file not found: " + resourceFilePath);
        }
        return inputStream;
    }

    /**
     * Validate the object read in from the JSON file.
     * This method also fills in additional properties of the objects, for example by parsing the string values in
     * the JSON object.
     */
    public void validate() {
        server.validate();

        for (TestScriptSpecification testScript : testScripts) {
            testScript.validate();
        }

        for (TestUserSpecification testUser : testUsers) {
            testUser.validate();
        }
    }

    public void display() {
        System.out.println("Test configuration: " + name);
        System.out.println();
        server.display();

        for (TestScriptSpecification testScript : testScripts) {
            System.out.println();
            testScript.display();
        }

        for (TestUserSpecification testUser : testUsers) {
            System.out.println();
            testUser.display();
        }

        System.out.println();
    }
}
