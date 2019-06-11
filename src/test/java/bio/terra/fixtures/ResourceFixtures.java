package bio.terra.fixtures;

import bio.terra.metadata.BillingProfile;

import java.util.Collections;
import java.util.Random;
import java.util.stream.Collectors;

public final class ResourceFixtures {
    private ResourceFixtures() {}

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
        return Collections.nCopies(3, 6)
            .stream()
            .map(ResourceFixtures::randomHex)
            .collect(Collectors.joining("-"));
    }

    public static BillingProfile randomBillingProfile() {
        return new BillingProfile()
            .biller("onix")
            .name("Random test profile")
            .billingAccountId(randomBillingAccountId());
    }
}
