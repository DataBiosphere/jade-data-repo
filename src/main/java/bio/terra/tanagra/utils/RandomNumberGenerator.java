package bio.terra.tanagra.utils;

import java.util.Random;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

/**
 * Utility for generating random numbers. Defined as a separate component so that tests can mock the
 * Spring bean. We can use an unseeded random in production and a seeded random in testing. This
 * allows us to produce an identical sequence for comparing expected/actual generated SQL strings.
 */
@Component
public class RandomNumberGenerator {
  private final Random random = new Random();

  @Bean
  public int getNext() {
    return Math.abs(random.nextInt(Integer.MAX_VALUE));
  }
}
