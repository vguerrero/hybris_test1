package concerttours.model;

import concerttours.util.FileHelper;
import concerttours.util.HsqlDBHelper;
import de.hybris.platform.testframework.HybrisJUnit4TransactionalTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class ConcerttoursModelTest extends HybrisJUnit4TransactionalTest {
    private static final Logger LOG = LoggerFactory.getLogger(ConcerttoursModelTest.class);

    @Before
    public void setUp() {
        // implement here code executed before each test
    }

    @After
    public void tearDown() {
        // implement here code executed after each test
    }

    /**
     * This is a sample test method.
     */
    @Test
    public void testModelClassesExist() throws IOException {
        String corePath = "C:\\hybris_test\\CXCOMM190500P_13-70004140\\hybris\\bin\\platform\\bootstrap\\gensrc\\de\\hybris\\platform\\core\\model\\product\\ProductModel.java";
        assertTrue("ProductModel has not been extended to support Hashtag and Band",
                FileHelper.fileContains(corePath,
                        "getHashtag", "getBand",
                        "setHashtag", "setBand"));

        String path = "C:\\hybris_test\\CXCOMM190500P_13-70004140\\hybris\\bin\\platform\\bootstrap\\gensrc\\concerttours\\model\\";
        assertTrue("The new BandModel does not support Code, Name, History, AlbumSales",
                FileHelper.fileContains(path + "BandModel.java",
                        "getName", "getHistory", "getCode", "getAlbumSales",
                        "setName", "setHistory", "setCode", "setAlbumSales"));

        assertTrue("The new ConcertModel does not extend VariantProductModel or does not support Venue and Date",
                FileHelper.fileContains(path + "ConcertModel.java",
                        "ConcertModel extends VariantProductModel",
                        "getVenue", "getDate",
                        "setVenue", "setDate"));


    }

    /*
    verify BAND table creation
     */
    @Test
    public void testDatabaseSetup() throws Exception {
        HsqlDBHelper hsqldb = new HsqlDBHelper(); // Note test will fail if the suite is running on this DB at the same time.
        try {
            String res = hsqldb.select("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.SYSTEM_COLUMNS WHERE TABLE_NAME NOT LIKE 'SYSTEM_%'");
            assertTrue("Could not find the table BANDS", res.contains("BANDS") );
        }
        catch (Exception e) {
            LOG.error("testDatabaseSetup" +  "HsqlDBTest failed: " + e.getMessage());
        }
        finally {
            hsqldb.shutdown();
        }
    }



}
