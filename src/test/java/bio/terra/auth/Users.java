package bio.terra.auth;

import bio.terra.integration.configuration.TestConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class Users {


    private TestConfiguration testConfig;
    private Map<String, List<Credentials>> userCredentialsByRole = new HashMap<>();
    private String password;
    private Users singleton;

    @Autowired
    public Users(TestConfiguration testConfig) {
        this.testConfig = testConfig;
        this.password = testConfig.getNotSoSecretPassword();
        buildUsersByRole(testConfig.getUsers());
    }

    private void buildUsersByRole(List<TestConfiguration.User> users) {
        users.stream().forEach(user -> {
            String role = user.getRole();
            List<Credentials> newList = new ArrayList<>();

            if (userCredentialsByRole.containsKey(role))
                newList = userCredentialsByRole.get(role);
            newList.add(new Credentials(user, password));
            userCredentialsByRole.put(role, newList);
        });
    }

    public Credentials getUserCredentialsForRole(String role) {
        // TODO randomize the user returned
        // isn't handling null
        return userCredentialsByRole.get(role).get(0);
    }



}
