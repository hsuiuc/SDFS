import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

/**monitor the node that need to send heartbeat to this node
 * detect possible failure
 * Created by haosun on 11/3/17.
 */
public class MonitorThread extends Thread{

    @Override
    public void run() {
        DatagramSocket sendSocket = null;
        while (sendSocket == null) {
            try {
                sendSocket = new DatagramSocket();
            } catch (SocketException e) {
                e.printStackTrace();
            }
        }

        List<String> timeoutNodes = new ArrayList<>();

        while (true) {
            try {
                Thread.sleep(500);

                boolean needUpdate = false;

                synchronized (Daemon.membershipList) {
                    synchronized (Daemon.neighbours) {
                        for (String neighbour : Daemon.neighbours) {
                            long lastAliveMoment = Daemon.membershipList.get(neighbour)[1];
                            if (System.currentTimeMillis() - lastAliveMoment > 2000) {
                                timeoutNodes.add(neighbour);
                                needUpdate = true;
                            } else {
                                Daemon.writeLog("PASS", neighbour);
                            }
                        }
                        if (needUpdate) {
                            for (String node : timeoutNodes) {
                                Daemon.membershipList.remove(node);
                                Daemon.writeLog("FAILURE", node);
                            }
                            Daemon.updateNeighbours();
                        }
                    }
                }

                if (needUpdate) {
                    for (String node : timeoutNodes) {
                        Protocol.sendGossip(node, "REMOVE", 0, 2, 2, sendSocket);
                        Daemon.writeLog("REMOVE", node);
                    }
                }
                timeoutNodes.clear();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
