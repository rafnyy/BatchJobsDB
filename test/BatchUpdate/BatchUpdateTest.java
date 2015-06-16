package BatchUpdate;

import static BatchUpdate.BatchUpdate.industryMap;
import static BatchUpdate.BatchUpdate.jobTitleMap;
import Database.DatabaseMock;
import Database.UserData;
import MessageQueue.SendMock;
import com.google.gson.Gson;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import jdbm.PrimaryTreeMap;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 *
 * @author Rob Fusco
 */
public class BatchUpdateTest extends TestCase {

    private static final String dbName = "JobDBTest";

    private static final String sdeJob = "Software Developer";
    private static final String managerJob = "Finance Manager";
    private static final String epJob = "Executive Producer";

    private static final String saasInd = "SaaS";
    private static final String aviInd = "Aviation";
    private static final String trInd = "Trampoline Robotics";

    public BatchUpdateTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(BatchUpdateTest.class);
        return suite;
    }

    @Override
    protected void setUp() throws Exception {
        DatabaseMock db = new DatabaseMock(dbName);
        db.update(0, "{\"name\":\"Rob\",\"job_title\":\"" + sdeJob + "\",\"industry\":\"" + saasInd + "\",\"version\":\"0\"}");
        db.update(1, "{\"name\":\"Adam\",\"job_title\":\"" + sdeJob + "\",\"industry\":\"" + saasInd + "\",\"version\":\"0\"}");
        db.update(2, "{\"name\":\"Heather\",\"job_title\":\"" + managerJob + "\",\"industry\":\"" + aviInd + "\",\"version\":\"0\"}");
        db.update(3, "{\"name\":\"Joe\",\"job_title\":\"" + sdeJob + "\",\"industry\":\"" + aviInd + "\",\"version\":\"0\"}");
        db.update(4, "{\"name\":\"Jane\",\"job_title\":\"" + epJob + "\",\"industry\":\"" + trInd + "\",\"version\":\"0\"}");
        db.update(5, "{\"name\":\"Rosaria\",\"job_title\":\"" + managerJob + "\",\"industry\":\"" + trInd + "\",\"version\":\"0\"}");

        BatchUpdate.prePopulateFieldsMap(db);

        db.close();
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        DatabaseMock db = new DatabaseMock(dbName);
        RecordManager recMan = RecordManagerFactory.createRecordManager(BatchUpdate.getFieldsMapName(db));
        PrimaryTreeMap<String, Set<Integer>> jobTitlesTree = recMan.treeMap(jobTitleMap);
        PrimaryTreeMap<String, Set<Integer>> industriesTree = recMan.treeMap(industryMap);

        // Delete FieldsMap
        Iterator<Map.Entry<String, Set<Integer>>> jobIter = jobTitlesTree.entrySet().iterator();
        while (jobIter.hasNext()) {
            jobTitlesTree.remove(jobIter.next().getKey());
        }

        Iterator<Map.Entry<String, Set<Integer>>> industryIter = industriesTree.entrySet().iterator();
        while (industryIter.hasNext()) {
            industriesTree.remove(industryIter.next().getKey());
        }

        recMan.close();
        // Delete DB
        Iterator<Map.Entry<Integer, String>> iter = db.scan(0);
        while (iter.hasNext()) {
            db.delete(iter.next().getKey());
        }
        db.close();
        super.tearDown();
    }

    /**
     * Test of prePopulateFieldsMap method, of class BatchUpdate.
     *
     * @throws java.io.IOException
     */
    public void testPrePopulateFieldsMap() throws IOException {
        System.out.println("prePopulateFieldsMap");
        // prePopulateFieldsMap(DatabaseMock db) already run in set up

        DatabaseMock db = new DatabaseMock(dbName);
        RecordManager recMan = RecordManagerFactory.createRecordManager(BatchUpdate.getFieldsMapName(db));
        PrimaryTreeMap<String, Set<Integer>> jobTitlesTree = recMan.treeMap(jobTitleMap);
        PrimaryTreeMap<String, Set<Integer>> industriesTree = recMan.treeMap(industryMap);

        Set<Map.Entry<String, Set<Integer>>> jobUserIds = jobTitlesTree.entrySet();
        // check that the job FieldsMap is correct
        Iterator<Map.Entry<String, Set<Integer>>> iter = jobUserIds.iterator();
        while (iter.hasNext()) {
            Map.Entry<String, Set<Integer>> entry = iter.next();
            Set<Integer> user_ids = new HashSet<>();
            switch (entry.getKey()) {
                case sdeJob:
                    user_ids.add(0);
                    user_ids.add(1);
                    user_ids.add(3);
                    assertEquals(entry.getValue(), user_ids);
                    break;
                case managerJob:
                    user_ids.add(2);
                    user_ids.add(5);
                    assertEquals(entry.getValue(), user_ids);
                    break;
                case epJob:
                    user_ids.add(4);
                    assertEquals(entry.getValue(), user_ids);
                    break;
                default:
                    break;
            }
        }

        Set<Map.Entry<String, Set<Integer>>> industryuserIds = industriesTree.entrySet();
        // check that the industry FieldsMap is correct
        iter = industryuserIds.iterator();
        while (iter.hasNext()) {
            Map.Entry<String, Set<Integer>> entry = iter.next();
            Set<Integer> user_ids = new HashSet<>();
            switch (entry.getKey()) {
                case saasInd:
                    user_ids.add(0);
                    user_ids.add(1);
                    assertEquals(entry.getValue(), user_ids);
                    break;
                case aviInd:
                    user_ids.add(2);
                    user_ids.add(3);
                    assertEquals(entry.getValue(), user_ids);
                    break;
                case trInd:
                    user_ids.add(4);
                    user_ids.add(5);
                    assertEquals(entry.getValue(), user_ids);
                    break;
                default:
                    break;
            }
        }
        recMan.close();
        db.close();
    }

    /**
     * Test of updateFieldsMap method, of class BatchUpdate.
     *
     * @throws java.lang.Exception
     */
    public void testUpdateFieldsMap() throws Exception {
        System.out.println("updateFieldsMap");
        DatabaseMock db = new DatabaseMock(dbName);

        final String paycheckJob = "Paycheck Collecter";
        final String fractureInd = "Fracture Fixers";

        SendMock.send(4, "{\"old_job_title\": \"" + epJob + "\", \"new_job_title\": \"" + paycheckJob + "\", \"version\": 10}");
        SendMock.send(5, "{\"old_industry\": \"" + trInd + "\", \"new_industry\": \"" + fractureInd + "\", \"version\": 10}");
        // Don't update FieldsMap if we get an old update from the queue for some reason
        SendMock.send(0, "{\"old_industry\": \"" + sdeJob + "\", \"new_industry\": \"Door to door digital delivery sales\", \"version\": 0}");

        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    BatchUpdate.updateFieldsMap(db);
                } catch (Exception ex) {
                    Logger.getLogger(BatchUpdateTest.class.getName()).log(Level.SEVERE, null, ex);
                    fail("Exception thrown subscribing to queue for DB " + db.getName());
                }
            }
        };
        t.start();

        //Extra long wait to be certain that all threads are finished editing FieldsMap
        Thread.sleep(10000);

        RecordManager recMan = RecordManagerFactory.createRecordManager(BatchUpdate.getFieldsMapName(db));
        PrimaryTreeMap<String, Set<Integer>> jobTitlesTree = recMan.treeMap(jobTitleMap);
        PrimaryTreeMap<String, Set<Integer>> industriesTree = recMan.treeMap(industryMap);

        Set<Map.Entry<String, Set<Integer>>> jobUserIds = jobTitlesTree.entrySet();
        // check that the job FieldsMap is correct
        Iterator<Map.Entry<String, Set<Integer>>> iter = jobUserIds.iterator();
        while (iter.hasNext()) {
            Map.Entry<String, Set<Integer>> entry = iter.next();
            Set<Integer> user_ids = new HashSet<>();
            switch (entry.getKey()) {
                case sdeJob:
                    user_ids.add(0);
                    user_ids.add(1);
                    user_ids.add(3);
                    assertEquals(entry.getValue(), user_ids);
                    break;
                case managerJob:
                    user_ids.add(2);
                    user_ids.add(5);
                    assertEquals(entry.getValue(), user_ids);
                    break;
                case paycheckJob:
                    user_ids.add(4);
                    assertEquals(entry.getValue(), user_ids);
                    break;
                default:
                    break;
            }
        }

        Set<Map.Entry<String, Set<Integer>>> industryuserIds = industriesTree.entrySet();
        // check that the industry FieldsMap is correct
        iter = industryuserIds.iterator();
        while (iter.hasNext()) {
            Map.Entry<String, Set<Integer>> entry = iter.next();
            Set<Integer> user_ids = new HashSet<>();
            switch (entry.getKey()) {
                case saasInd:
                    user_ids.add(0);
                    user_ids.add(1);
                    assertEquals(entry.getValue(), user_ids);
                    break;
                case aviInd:
                    user_ids.add(2);
                    user_ids.add(3);
                    assertEquals(entry.getValue(), user_ids);
                    break;
                case trInd:
                    user_ids.add(4);
                    assertEquals(entry.getValue(), user_ids);
                    break;
                case fractureInd:
                    user_ids.add(5);
                    assertEquals(entry.getValue(), user_ids);
                    break;
                default:
                    break;
            }
        }
        recMan.close();
        db.close();
    }

    /**
     * Test of change_job_title method, of class BatchUpdate.
     *
     * @throws java.lang.Exception
     */
    public void testChange_job_title() throws Exception {
        System.out.println("change_job_title");
        DatabaseMock db = new DatabaseMock(dbName);

        final String newJob = "Code Monekey";
        Gson gson = new Gson();

        Set<Integer> changedUserIds = new HashSet<>();
        Iterator<Map.Entry<Integer, String>> iter = db.scan(0);
        while (iter.hasNext()) {
            Map.Entry<Integer, String> entry = iter.next();
            String json_data = entry.getValue();
            if (json_data != null) {
                UserData user_data = gson.fromJson(json_data, UserData.class);
                if (user_data.getJobTitle().equals(sdeJob)) {
                    changedUserIds.add(entry.getKey());
                }
            }
        }

        BatchUpdate.change_job_title(db, sdeJob, newJob);

        //Extra long wait to be certain that all threads are finished editing DB
        Thread.sleep(10000);

        iter = db.scan(0);
        while (iter.hasNext()) {
            Map.Entry<Integer, String> entry = iter.next();
            String json_data = entry.getValue();
            if (json_data != null) {
                UserData user_data = gson.fromJson(json_data, UserData.class);
                if (changedUserIds.contains(entry.getKey())) {
                    //Everything set to old value is now the new value
                    assertEquals(user_data.getJobTitle(), newJob);
                } else { //Nothing left in the DB is set to the old value
                    assert (!user_data.getJobTitle().equals(sdeJob));
                }
            }
        }
        db.close();
    }

    /**
     * Test of change_industry method, of class BatchUpdate.
     *
     * @throws java.lang.Exception
     */
    public void testChange_industry() throws Exception {
        System.out.println("change_industry");
        DatabaseMock db = new DatabaseMock(dbName);

        final String newIndustry = "aeroplane makin'";
        Gson gson = new Gson();

        Set<Integer> changedUserIds = new HashSet<>();
        Iterator<Map.Entry<Integer, String>> iter = db.scan(0);
        while (iter.hasNext()) {
            Map.Entry<Integer, String> entry = iter.next();
            String json_data = entry.getValue();
            if (json_data != null) {
                UserData user_data = gson.fromJson(json_data, UserData.class);
                if (user_data.getJobTitle().equals(aviInd)) {
                    changedUserIds.add(entry.getKey());
                }
            }
        }

        BatchUpdate.change_job_title(db, aviInd, newIndustry);

        //Extra long wait to be certain that all threads are finished editing DB
        Thread.sleep(10000);

        iter = db.scan(0);
        while (iter.hasNext()) {
            Map.Entry<Integer, String> entry = iter.next();
            String json_data = entry.getValue();
            if (json_data != null) {
                UserData user_data = gson.fromJson(json_data, UserData.class);
                if (changedUserIds.contains(entry.getKey())) {
                    //Everything set to old value is now the new value
                    assertEquals(user_data.getJobTitle(), newIndustry);
                } else { //Nothing left in the DB is set to the old value
                    assert (!user_data.getJobTitle().equals(aviInd));
                }
            }
        }
        db.close();
    }

    /**
     * Test of getFieldsMapName method, of class BatchUpdate.
     * @throws java.io.IOException
     */
    public void testGetFieldsMapName() throws IOException {
        System.out.println("getFieldsMapName");
        DatabaseMock db = new DatabaseMock("DBname");
        String expResult = "FieldsMap4" + db.getName();
        String result = BatchUpdate.getFieldsMapName(db);
        assertEquals(expResult, result);
    }
}