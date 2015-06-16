package Database;

import com.google.gson.Gson;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;

/**
 * This database stores user information in JSON documents that have the
 * following fields: name (string) job_title (string) industry (string) version
 * (long integer)
 *
 * Since mock, for unit testing assume all functions are perfect
 * @author Rob Fusco
 */
public class DatabaseMock {

    private final String name;
    private final RecordManager recMan;
    private final PrimaryTreeMap<Integer, String> db;

    public DatabaseMock(String name) throws IOException {
        this.name = name;
        recMan = RecordManagerFactory.createRecordManager(name);
        db = recMan.treeMap("user_id");
    }

    /**
     * Returns a JSON document that represents a set of user information
     *
     * @param user_id
     * @return
     */
    public String read(int user_id) {
        return db.get(user_id);
    }

    /**
     * Allows you to completely replace/create user information by sending a
     * JSON document. Returns the updated version ID of the document.
     *
     * @param user_id
     * @param user_data
     * @return
     * @throws java.util.concurrent.TimeoutException
     */
    public long update(int user_id, String user_data) throws TimeoutException {
        db.put(user_id, user_data);
        Gson gson = new Gson();
        return gson.fromJson(user_data, UserData.class).getVersion();
    }

    /**
     * Delete user data entirely
     *
     * @param user_id
     */
    public void delete(int user_id) {
        db.remove(user_id);
    }

    /**
     * Iterate through all the rows of the database, starting at the given user
     * ID (buffered). The user ID does not need to exist (the database will
     * start at the next higher existing user ID)
     *
     * @param user_id
     * @return
     */
    public Iterator<Map.Entry<Integer, String>> scan(int user_id) {
        //Switch entrySet to List so it is ordered
        Set<Map.Entry<Integer, String>> set = db.entrySet();
        List<Map.Entry<Integer, String>> list = new ArrayList<>();
        list.addAll(set);

        //Find index of list that is equivalent to user_id entered
        Iterator<Map.Entry<Integer, String>> iter = list.iterator();
        boolean found = false;
        int i = 0;
        while (iter.hasNext() && !found) {
            Map.Entry<Integer, String> next = iter.next();
            if (next.getKey() < user_id) {
                i++;
            } else {
                found = true;
            }
        }

        //Rebuild list in new order with entry represented by user_id first
        List<Map.Entry<Integer, String>> reordered = new ArrayList<>();
        reordered.addAll(list.subList(i, list.size()));
        reordered.addAll(list.subList(0, i));

        //Return an iterator of newly order list
        return reordered.iterator();
    }

    public void close() throws IOException {
        recMan.close();
    }

    public String getName() {
        return name;
    }
}