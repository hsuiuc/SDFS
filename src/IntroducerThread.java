import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.Map;

/**
 * Created by haosun on 11/3/17.
 */
public class IntroducerThread extends Thread {

    @Override
    public void run() {
        //init introducer socket, listen to join requests
        DatagramSocket introducerSocket = null;
        while (introducerSocket == null) {
            try {
                introducerSocket = new DatagramSocket(Daemon.joinPortNumber);
            } catch (SocketException e) {
                e.printStackTrace();
            }
        }

        //store the ID of the nodes requesting join
        byte[] receiveData = new byte[1024];
        //store membership list to send to the new join node
        byte[] sendData;

        while (true) {
            try {
                DatagramPacket introducerReceivePacket = new DatagramPacket(receiveData, receiveData.length);
                introducerSocket.receive(introducerReceivePacket);
                String joinNodeID = new String(introducerReceivePacket.getData(), 0, introducerReceivePacket.getLength());
                Daemon.membershipList.put(joinNodeID, new long[]{0, System.currentTimeMillis()});

                //build the membership list into a string
                //send the string to the join node
                //message format
                //nodeID/counter%anothernodeID/counter
                StringBuilder sb = new StringBuilder();
                synchronized (Daemon.membershipList) {
                    for (Map.Entry<String, long[]> mapEntry : Daemon.membershipList.entrySet()) {
                        //append ID
                        sb.append(mapEntry.getKey());
                        sb.append("/");
                        //append counter
                        sb.append(mapEntry.getValue()[0]);
                        sb.append("%");
                    }
                }

                //send the string to the joining node
                sendData = sb.toString().getBytes();
                InetAddress joinNodeAddress = InetAddress.getByName(joinNodeID.split("#")[1]);
                DatagramPacket introducerSendPacket = new DatagramPacket(sendData, sendData.length, joinNodeAddress, introducerReceivePacket.getPort());
                introducerSocket.send(introducerSendPacket);

                //update neighbours
                Daemon.updateNeighbours();

                //write log
                Daemon.writeLog("INITIAL ADD", joinNodeID);

                //gossip the new join to all the nodes
                Protocol.sendGossip(joinNodeID, "ADD", 0, 2, 2, introducerSocket);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
