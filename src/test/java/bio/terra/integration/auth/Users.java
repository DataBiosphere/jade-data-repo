package bio.terra.integration.auth;

import bio.terra.integration.configuration.TestConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@ActiveProfiles({"google", "integrationtest"})
public class Users {


    private Map<String, List<Credentials>> userCredentialsByRole = new HashMap<>();
//    private String password;

    @Autowired
    public Users(TestConfiguration testConfig) {
//        this.password = testConfig.getNotSoSecretPassword();
        buildUsersByRole(testConfig.getUsers());
    }

    private void buildUsersByRole(List<TestConfiguration.User> users) {
        users.stream().forEach(user -> {
            String role = user.getRole();
            List<Credentials> newList = new ArrayList<>();

            if (userCredentialsByRole.containsKey(role))
                newList = userCredentialsByRole.get(role);
            newList.add(new Credentials(user));
            userCredentialsByRole.put(role, newList);
        });
    }

    public Credentials getUserCredentialsForRole(String role) {
        // TODO randomize the user returned
        // isn't handling null
        return userCredentialsByRole.get(role).get(0);
    }



}
