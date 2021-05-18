package bio.terra.service.tabulardata.google;


import bio.terra.common.category.Unit;

import org.junit.After;

import org.junit.Before;

import org.junit.experimental.categories.Category;

import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;

import org.springframework.test.context.junit4.SpringRunner;


@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class BigQueryPdaoUnitTest {
    private static final Logger logger = LoggerFactory.getLogger(BigQueryPdaoUnitTest.class);


    @Before
    public void setup() throws Exception {
    }

    @After
    public void teardown() throws Exception {
    }


    //TODO - Snapshot by Asset
    //Test BigQueryPdao.mapValuesToRows()




}
