import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**send heart beat and send gossip
 * Created by haosun on 11/3/17.
 */
public class Protocol {
    /**
     * send heartbeat to all neighbours
     * @param ID node ID
     * @param counter heartbeat counter
     * @param sendSocket send socket
     */
    public static void sendHeartBeat(String ID, long counter, DatagramSocket sendSocket) {
        byte[] heartBeatMessage = ("0_" + ID + "_" + counter).getBytes();

        for (String neighbour : Daemon.neighbours) {
            try {
                DatagramPacket heartBeatPacket = new DatagramPacket(heartBeatMessage, heartBeatMessage.length,
                        InetAddress.getByName(neighbour.split("#")[1]), Daemon.packetPortNumber);
                sendSocket.send(heartBeatPacket);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * send gossip message to randomly chosen members
     * @param ID node id
     * @param action action
     * @param counter heartbeat counter
     * @param TTL relay number
     * @param numOfTarget num of members to send gossip to
     * @param sendSocket the socket used to send the message
     */
    public static void sendGossip(String ID, String action, long counter, int TTL, int numOfTarget, DatagramSocket sendSocket) {
        byte[] gossipMessage = ("1_" + ID + "_" + action + "_" + counter + "_" + TTL).getBytes();
        int membershipListSize = Daemon.membershipList.size();
        List<Integer> randomIndex = new ArrayList<>();
        for (int i = 0; i < membershipListSize; i++) {
            randomIndex.add(i);
        }
        Collections.shuffle(randomIndex);

        Object[] memberIDs = Daemon.membershipList.keySet().toArray();
        for (int i = 0; i < Math.min(numOfTarget, membershipListSize); i++) {
            try {
                InetAddress targetAddress = InetAddress.getByName(((String) memberIDs[randomIndex.get(i)]).split("#")[1]);
                DatagramPacket gossipPacket = new DatagramPacket(gossipMessage, gossipMessage.length,
                        targetAddress, Daemon.packetPortNumber);
                sendSocket.send(gossipPacket);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
