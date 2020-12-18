package bio.terra;

import bio.terra.common.category.Unit;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Random;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class FlakyUnitTest {
    private static final Logger logger = LoggerFactory.getLogger(FlakyUnitTest.class);

    @Test
    public void flakyTest() throws Exception {
        Random rand = new Random();

        // Generate random integers in range 0 to 9
        int rand_int1 = rand.nextInt(10);
        if (rand_int1 < 5){
            logger.info("Random Failure");
            throw new Exception("random failure");
        } else {
         logger.info("Random Pass");
        }

    }
}
