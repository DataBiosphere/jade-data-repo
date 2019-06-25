package bio.terra.fixtures;

import bio.terra.metadata.BillingProfile;
import bio.terra.model.BillingProfileRequestModel;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public final class ProfileFixtures {
    private ProfileFixtures() {}

    public static String randomHex(int n) {
        Random r = new Random();
        StringBuffer sb = new StringBuffer();
        while (sb.length() < n) {
            sb.append(Integer.toHexString(r.nextInt(16)));
        }
        return sb.toString();
    }

    // Google billing account ids are three sets of six capitalized hex characters separated by dashes
    public static String randomBillingAccountId() {
        List<String> groups = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            groups.add(randomHex(6));
        }
        return String.join("-", groups).toUpperCase();
    }

    public static BillingProfile randomBillingProfile() {
        return new BillingProfile()
            .biller("onix")
            .name("Random test profile")
            .billingAccountId(randomBillingAccountId());
    }

    public static BillingProfileRequestModel randomBillingProfileRequest() {
        return new BillingProfileRequestModel()
            .billingAccountId(ProfileFixtures.randomBillingAccountId())
            .biller("direct")
            .profileName("Test billing profile");
    }
}
