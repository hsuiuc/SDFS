import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

/**listen to incoming messages, do certain options
 * two kinds of messages:
 * 0_...:heartbeat messages, sent by heartbeat thread
 * 1_...:gossip messages, three kinds
 * 0:add, sent by introducer thread, new node join the group
 * 1:leave, sent by voluntarily leave command
 * 2:remove, sent by monitor thread, didn't receive heartbeat, node is suspected to be down
 * Created by haosun on 11/3/17.
 */
public class ListeningThread extends Thread {
    //socket listening to incoming messages
    private DatagramSocket serverSocket;
    //socket to relay gossip if necessary
    private DatagramSocket sendSocket;

    ListeningThread() {
        try {
            //init server socket, listening on packetPortNumber
            serverSocket = new DatagramSocket(Daemon.packetPortNumber);
            sendSocket = new DatagramSocket();
        } catch (SocketException e) {
            e.printStackTrace();
        }
    }

    /**
     * update membership list according to received message type
     * four types: "HB", "ADD", "LEAVE", "REMOVE"
     * @param ID node ID
     * @param messageType message type
     * @param counter heartbeat counter
     */
    private void updateMembershipList(String ID, String messageType, long counter) {
        synchronized (Daemon.membershipList) {
            long[] memberDetail = Daemon.membershipList.get(ID);
            switch (messageType) {
                case "HEARTBEAT":
                    if (memberDetail == null) {
                        Daemon.membershipList.put(ID, new long[]{counter, System.currentTimeMillis()});
                        Daemon.updateNeighbours();
                        Protocol.sendGossip(ID, "ADD", counter, 2, 2, sendSocket);
                        Daemon.writeLog("HEARTBEAT REJOIN", ID);
                    } else if (counter > memberDetail[0]) {
                        Daemon.membershipList.put(ID, new long[]{counter, System.currentTimeMillis()});
                        Daemon.writeLog("HEARTBEAT NEIGHBOR", ID);
                    }
                    break;
                case "ADD":
                    if (memberDetail == null || counter > memberDetail[0]) {
                        Daemon.membershipList.put(ID, new long[]{counter, System.currentTimeMillis()});
                        if (memberDetail == null) {
                            Daemon.updateNeighbours();
                            Daemon.writeLog("GOSSIP ADD", ID);
                        }
                    }
                    break;
                case "LEAVE":
                    if (memberDetail != null) {
                        Daemon.membershipList.remove(ID);
                        Daemon.updateNeighbours();
                        Daemon.writeLog("GOSSIP LEAVE", ID);
                    }
                    break;
                case "REMOVE":
                    if (memberDetail != null) {
                        Daemon.membershipList.remove(ID);
                        Daemon.updateNeighbours();
                        Daemon.writeLog("GOSSIP REMOVE", ID);
                    }
                    break;
                default:
                    System.err.println("unknown message type : " + messageType);
                    System.exit(1);
                    break;
            }
        }
    }

    @Override
    public void run() {
        byte[] receivedMessage = new byte[1024];

        while (true) {
            try {
                DatagramPacket receivedPacket = new DatagramPacket(receivedMessage, receivedMessage.length);
                serverSocket.receive(receivedPacket);
                String message = new String(receivedPacket.getData(), 0, receivedPacket.getLength());
                String[] parsedMessage = message.split("_");
                String messageFlag = parsedMessage[0];
                switch (messageFlag) {
                    case "0":
                        updateMembershipList(parsedMessage[1], "HEARTBEAT", Long.parseLong(parsedMessage[2]));
                        //Daemon.writeLog("HEARTBEAT", parsedMessage[1]);
                        break;
                    case "1":
                        int TTL = Integer.parseInt(parsedMessage[4]);
                        if (TTL > 1) {
                            Protocol.sendGossip(parsedMessage[1], parsedMessage[2], Long.parseLong(parsedMessage[3]),
                                    --TTL, 2, sendSocket);
                        }
                        updateMembershipList(parsedMessage[1], parsedMessage[2], Long.parseLong(parsedMessage[3]));
                        //Daemon.writeLog("GOSSIP", parsedMessage[1]);
                        break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
