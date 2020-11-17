package bio.terra.common.fixtures;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;

import java.util.*;

public final class ResourceFixtures {
    private ResourceFixtures() {}

    // Build the a random project resource given the billing profile in support of
    // DAO-only unit tests. This is ready to be used as a request to the GoogleResourceDao
    public static GoogleProjectResource randomProjectResource(BillingProfileModel billingProfile) {
        return new GoogleProjectResource()
            .googleProjectId("fake-test-project-id")
            .googleProjectNumber("123456")
            .profileId(UUID.fromString(billingProfile.getId()));
    }

    public static GoogleProjectResource randomProjectResourceAndName(BillingProfileModel billingProfile) {

        return new GoogleProjectResource()
            .googleProjectId(ProfileFixtures.randomizeName("fake-test-project-id"))
            .googleProjectNumber(shuffleString("123456"))
            .profileId(UUID.fromString(billingProfile.getId()));
    }

    public static String shuffleString(String input) {
        List<String> newProjectNum = Arrays.asList(input.split(""));
        Collections.shuffle(newProjectNum);
        String shuffled = "";
        for (String let : newProjectNum) {
            shuffled += let;
        }
        return shuffled;
    }



}
