package Database;

import junit.framework.TestCase;

/**
 *
 * @author Rob Fusco
 */
public class UserDataTest extends TestCase {
    private static UserData bestJobInTheWorld; //according to Wikipedia
        
    private static final String name = "Ben Southall";
    private static final String job_title = "Caretaker of the Islands";
    private static final String industry = "Hamilton Island Tourism";
    
    // Worst job, according to... the Daily Mirror... ugh
    private static final String worstName = "Karina Acevedo-Whitehouse";
    private static final String worstJob_title = "Whale snot collector";
    private static final String worstIndustry = "Zoology";
    
    @Override
    protected void setUp() throws Exception {
        bestJobInTheWorld = new UserData(name, job_title, industry); 
        super.setUp();
    }

    /**
     * Test of getName method, of class UserData.
     */
    public void testGetName() {
        System.out.println("getName");
        String result = bestJobInTheWorld.getName();
        assertEquals(name, result);
    }

    /**
     * Test of setName method, of class UserData.
     */
    public void testSetName() {
        System.out.println("setName");
        bestJobInTheWorld.setName(worstName);
        assertEquals(bestJobInTheWorld.getName(), worstName);
    }

    /**
     * Test of getJobTitle method, of class UserData.
     */
    public void testGetJobTitle() {
        System.out.println("getJobTitle");
        String result = bestJobInTheWorld.getJobTitle();
        assertEquals(job_title, result);
    }

    /**
     * Test of setJobTitle method, of class UserData.
     */
    public void testSetJobTitle() {
        System.out.println("setJobTitle");
        bestJobInTheWorld.setJobTitle(worstJob_title);
        assertEquals(bestJobInTheWorld.getJobTitle(), worstJob_title);
    }

    /**
     * Test of getIndustry method, of class UserData.
     */
    public void testGetIndustry() {
        System.out.println("getIndustry");
        String result = bestJobInTheWorld.getIndustry();
        assertEquals(industry, result);
    }

    /**
     * Test of setIndustry method, of class UserData.
     */
    public void testSetIndustry() {
        System.out.println("setIndustry");
        bestJobInTheWorld.setIndustry(worstIndustry);
        assertEquals(bestJobInTheWorld.getIndustry(), worstIndustry);
    }

    /**
     * Test of getVersion method, of class UserData.
     * Version changes when setting other fields.
     * Must make sure it is incremented for all other setters.
     */
    public void testGetVersion() {
        System.out.println("getVersion");
        long result = bestJobInTheWorld.getVersion();
        assertEquals(1, result);
        
        bestJobInTheWorld.setName(worstName);
        assertEquals(bestJobInTheWorld.getVersion(), 2);
        
        bestJobInTheWorld.setName(worstJob_title);
        assertEquals(bestJobInTheWorld.getVersion(), 3);
        
        bestJobInTheWorld.setName(worstIndustry);
        assertEquals(bestJobInTheWorld.getVersion(), 4);
        // Sorry Ben, it was a good run
    }  
}