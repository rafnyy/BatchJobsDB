package BatchUpdate;

import Database.DatabaseMock;
import Database.UserData;
import Database.UserData.FieldType;
import MessageQueue.MessageQueueMock;
import MessageQueue.MessageQueueMock.CallBack;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import java.lang.reflect.Type;

/**
 *
 * @author Rob Fusco
 */
public class BatchUpdate {
    // Tables in the fields map DB that represent job_title or industry to user_id mapping.
    // Each table (or PrimaryTreeMap) is a fields map. A fields map is simply a persisted hash map
    // where the key is the value of job_title or industry and the value is 
    // a unique set of user_ids
    public static final String jobTitleMap = "jobTitle";
    public static final String industryMap = "industryTitle";

    /**
     * Builds a DB that represents a fields map.
     * A fields map has either a job_title or industry as the key, and a unique
     * Set of user_ids as the value. This allows us to grab all user_ids that have
     * a certain job_title or industry quickly and without accessing the main DB.
     * Fields maps are persisted to disk using JDBM because it is a lightweight 
     * and fast way to write what are essentially only just hash maps to disk.
     * 
     * This method only needs to be run once ever during the applications life
     * cycle per DB. updateFieldsMap(DatabaseMock db) can update it based on what
     * comes off the message queue.
     * 
     * This method can safely be run again regardless of what data exists and should
     * always persist accurate data.
     * @param db
     * @throws IOException 
     */
    public static void prePopulateFieldsMap(DatabaseMock db) throws IOException {
        Gson gson = new Gson();
        RecordManager recMan = RecordManagerFactory.createRecordManager(getFieldsMapName(db));
        PrimaryTreeMap<String, Set<Integer>> jobTitlesTree = recMan.treeMap(jobTitleMap);
        PrimaryTreeMap<String, Set<Integer>> industriesTree = recMan.treeMap(industryMap);

        Iterator<Map.Entry<Integer, String>> iter = db.scan(0);
        while (iter.hasNext()) {
            Map.Entry<Integer, String> entry = iter.next();
            int user_id = entry.getKey();
            String json_data = entry.getValue();
            if (json_data != null) {
                UserData user_data = gson.fromJson(json_data, UserData.class);
                addToFieldsMap(jobTitlesTree, user_data.getJobTitle(), user_id);
                addToFieldsMap(industriesTree, user_data.getIndustry(), user_id);
                recMan.commit();
            }
        }
        recMan.close();
    }

    /**
     * Write a single user_id to a table (defined by PrimaryTreeMap<String, Set<Integer>> tree)
     * into the fields map for either a job_title or industry.
     * 
     * If tables exist for name or other fields that have yet to be defined this method
     * should still work as long as the right PrimaryTreeMap<String, Set<Integer>>
     * is provided.
     * 
     * @param tree
     * @param value
     * @param user_id 
     */
    private static void addToFieldsMap(PrimaryTreeMap<String, Set<Integer>> tree, String value, int user_id) {
        Set<Integer> user_ids = tree.get(value);
        if (user_ids == null) {
            Set<Integer> new_user_ids = new HashSet<>();
            new_user_ids.add(user_id);
            tree.put(value, new_user_ids);
        } else {
            user_ids.add(user_id);
            tree.put(value, user_ids);
        }
    }

    /**
     * Removes a user_id for the value from the fields map based on PrimaryTreeMap<String, Set<Integer>> tree
     * Used when the user document has been updated and the fields map needs to 
     * be updated to reflect the document change.
     * 
     * @param tree
     * @param value
     * @param user_id 
     */
    private static void removeFromFieldsMap(PrimaryTreeMap<String, Set<Integer>> tree, String value, int user_id) {
        Set<Integer> user_ids = tree.get(value);
        if (user_ids != null) {
            user_ids.remove(user_id);
            tree.put(value, user_ids);
        }
    }

    /**
     * Static method that subscribes to the message queue to listen and respond
     * to any DB changes.
     * @param db
     * @return
     * @throws Exception 
     */
    public static MessageQueueMock updateFieldsMap(DatabaseMock db) throws Exception {
        MessageQueueMock recv = new MessageQueueMock();
        CallBack callback = new UpdateFieldMap(db);
        recv.subscribe_to_updates(callback);
        return recv;
    }

    /**
     * Class that defines what to do for any received messages from the message
     * queue. Pass this class as a parameter to the subscription method to define
     * what the callback behavior is.
     * 
     */
    static class UpdateFieldMap implements CallBack {
        DatabaseMock db;

        private UpdateFieldMap(DatabaseMock db) {
            this.db = db;
        }

        /**
         * Apply changes that have already been applied to the jobs DB to the
         * fields map to keep it accurate and up to date.
         * @param user_id
         * @param delta 
         */
        @Override
        public void callback_function(int user_id, String delta) {
            Type keyValueSet = new TypeToken<Map<String, String>>() {
            }.getType();

            Gson gson = new Gson();
            Map<String, String> deltaSet = gson.fromJson(delta, keyValueSet);

            boolean moreRecent = true;
            try {
                RecordManager recMan = RecordManagerFactory.createRecordManager(getFieldsMapName(db));
                for (Map.Entry<String, String> entry : deltaSet.entrySet()) {
                    if (entry.getKey().equals("version")) {
                        // Check that version > latest version in DB
                        String json = db.read(user_id);
                        UserData user_data = gson.fromJson(json, UserData.class);
                        // If the version in the DB is higher than the version from the queue
                        // then we have invalid or old message from the queue so do not update
                        // the fields map
                        if (user_data.getVersion() >= Integer.parseInt(entry.getValue())) {
                            moreRecent = false;
                        }
                    } else if (entry.getKey().startsWith("old_")) {
                        String type = entry.getKey().substring(4);
                        switch (type) {
                            case "job_title":
                                PrimaryTreeMap<String, Set<Integer>> jobTitlesTree = recMan.treeMap(jobTitleMap);
                                removeFromFieldsMap(jobTitlesTree, entry.getValue(), user_id);
                                break;
                            case "industry":
                                PrimaryTreeMap<String, Set<Integer>> industriesTree = recMan.treeMap(industryMap);
                                removeFromFieldsMap(industriesTree, entry.getValue(), user_id);
                                break;
                            case "name":
                                break;
                            case "version":
                                break;
                            default:
                                break;
                        }
                    } else if (entry.getKey().startsWith("new_")) {
                        String type = entry.getKey().substring(4);
                        switch (type) {
                            case "job_title":
                                PrimaryTreeMap<String, Set<Integer>> jobTitlesTree = recMan.treeMap(jobTitleMap);
                                addToFieldsMap(jobTitlesTree, entry.getValue(), user_id);
                                break;
                            case "industry":
                                PrimaryTreeMap<String, Set<Integer>> industriesTree = recMan.treeMap(industryMap);
                                addToFieldsMap(industriesTree, entry.getValue(), user_id);
                                break;
                            case "name":
                                break;
                            case "version":
                                break;
                            default:
                                break;
                        }
                    }
                }
                if (moreRecent) {
                    recMan.commit();
                }
                recMan.close();
            } catch (IOException ex) {
                Logger.getLogger(BatchUpdate.class.getName()).log(Level.SEVERE, "Error accessing Fields Map", ex);
            }
        }
    }

    public static void change_job_title(DatabaseMock db, String oldJob, String newJob) throws IOException {
        change_field(db, oldJob, newJob, FieldType.job_title);
    }

    public static void change_industry(DatabaseMock db, String oldIndustry, String newIndustry) throws IOException {
        change_field(db, oldIndustry, newIndustry, FieldType.industry);
    }

    private static void change_field(DatabaseMock db, String oldVal, String newVal, FieldType fieldType) throws IOException {
        Set<Integer> entries = findMatches(db, oldVal, fieldType);

        if (entries != null) {
            Iterator<Integer> iter = entries.iterator();
            while (iter.hasNext()) {
                int value = iter.next();
                // One DB write doesn't need to wait for the next to finish before
                // it can start, spin all DB write off into their own thread.
                // May need to determine when Java will fail to to too many
                // threads and rework this concept.
                Thread t = new Thread() {
                    @Override
                    public void run() {
                        change_single_field(db, value, newVal, fieldType);
                    }
                };
                t.start();
            }
        }
    }

    /**
     *
     * @param db
     * @param val
     * @param fieldType
     * @return an array of user_ids, where the value of column fieldType is equal to
     * val, returns null if not matches or inapplicable
     */
    private static Set<Integer> findMatches(DatabaseMock db, String val, FieldType fieldType) throws IOException {
        Set<Integer> user_ids = null;
        RecordManager recMan = RecordManagerFactory.createRecordManager(getFieldsMapName(db));

        switch (fieldType) {
            case job_title:
                PrimaryTreeMap<String, Set<Integer>> jobTitlesDB = recMan.treeMap(jobTitleMap);
                user_ids = jobTitlesDB.get(val);
                break;
            case industry:
                PrimaryTreeMap<String, Set<Integer>> industryDB = recMan.treeMap(industryMap);
                user_ids = industryDB.get(val);
                break;
            case name:
                break;
            case version:
                break;
            default:
                break;
        }

        recMan.close();
        return user_ids;
    }

    private static void change_single_field(DatabaseMock db, int user_id, String val, FieldType fieldType) {
        Gson gson = new Gson();
        String user_data_json = db.read(user_id);

        UserData user_data = gson.fromJson(user_data_json, UserData.class);
        switch (fieldType) {
            case name:
                user_data.setName(val);
                break;
            case job_title:
                user_data.setJobTitle(val);
                break;
            case industry:
                user_data.setIndustry(val);
                break;
            default:
                break;
        }

        boolean updated;
        do {
            try {
                db.update(user_id, gson.toJson(user_data));
                updated = true;
            } catch (TimeoutException ex) {
                Logger.getLogger(BatchUpdate.class.getName()).log(Level.WARNING, null, ex);
                updated = false;
            }
        } while (!updated);
    }

    /**
     * Convenience method, consistently name the FieldsMap based on the DB it is referencing 
     * @param db
     * @return a FieldsMap name with the DB name appended to the end
     */
    public static String getFieldsMapName(DatabaseMock db) {
        return "FieldsMap4" + db.getName();
    }
}