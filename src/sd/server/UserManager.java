package sd.server;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class UserManager {
    private final Map<String, String> users;
    private final File storageFile;

    public UserManager() {
        this.users = new HashMap<>();
        this.storageFile = new File("users.dat");
        loadUsers();
    }

    private void loadUsers() {
        if (!storageFile.exists()) {
            return;
        }
        try (DataInputStream in = new DataInputStream(new FileInputStream(storageFile))) {
            int n = in.readInt();
            for (int i = 0; i < n; i++) {
                String username = in.readUTF();
                String password = in.readUTF();
                users.put(username, password);
            }
        } catch (IOException e) {
        }
    }

    private void saveUsers() {
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(storageFile))) {
            out.writeInt(users.size());
            for (Map.Entry<String, String> entry : users.entrySet()) {
                out.writeUTF(entry.getKey());
                out.writeUTF(entry.getValue());
            }
        } catch (IOException e) {
        }
    }

    public synchronized boolean register(String username, String password) {
        if (users.containsKey(username)) {
            return false;
        }
        users.put(username, password);
        saveUsers();
        return true;
    }

    public synchronized boolean authenticate(String username, String password) {
        String stored = users.get(username);
        if (stored == null) {
            return false;
        }
        return stored.equals(password);
    }
}
