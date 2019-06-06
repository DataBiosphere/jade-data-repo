package bio.terra.integration.auth;

import bio.terra.integration.configuration.TestConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@ActiveProfiles({"google", "integrationtest"})
public class Users {


    private Map<String, List<TestConfiguration.User>> usersByRole = new HashMap<>();

    @Autowired
    public Users(TestConfiguration testConfig) {
        buildUsersByRole(testConfig.getUsers());
    }

    private void buildUsersByRole(List<TestConfiguration.User> users) {
        users.stream().forEach(user -> {
            String role = user.getRole();
            List<TestConfiguration.User> newList = new ArrayList<>();

            if (usersByRole.containsKey(role))
                newList = usersByRole.get(role);
            newList.add(user);
            usersByRole.put(role, newList);
        });
    }

    public TestConfiguration.User getUserForRole(String role) {
        return getUsersForRole(role, 1).get(0);
    }

    public List<TestConfiguration.User> getUsersForRole(String role, int numUsers) {
        if (role == null) {
            throw new RuntimeException("Role not specified");
        }
        List<TestConfiguration.User> usersList = usersByRole.get(role);
        if (usersList.size() < numUsers) {
            throw new RuntimeException("not enough users for " + role);
        }
        Collections.shuffle(usersList);
        return usersList.subList(0, numUsers);
    }



}
