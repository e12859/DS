package sd.client.ui;

import sd.common.ProtocolConstants;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class NonConsumingClient {
    public static void main(String[] args) {
        String host = "localhost";
        int port = 5000;
        String username = "nonconsumer";
        String password = "nonconsumer";
        try {
            Socket socket = new Socket(host, port);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            out.writeByte(ProtocolConstants.MSG_REGISTER);
            out.writeUTF(username);
            out.writeUTF(password);
            out.flush();

            out.writeByte(ProtocolConstants.MSG_LOGIN);
            out.writeUTF(username);
            out.writeUTF(password);
            out.flush();

            int ops = 100000;
            for (int i = 0; i < ops; i++) {
                out.writeByte(ProtocolConstants.MSG_ADD_SALE);
                out.writeUTF("prod" + (i % 10));
                out.writeInt(1);
                out.writeDouble(1.0);
                out.flush();
                if (i % 1000 == 0) {
                    System.out.println("Sent " + i + " requests");
                }
            }

            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
