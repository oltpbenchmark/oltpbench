package com.oltpbenchmark.benchpress;

import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.corundumstudio.socketio.AckRequest;
import com.corundumstudio.socketio.SocketIOClient;
import com.corundumstudio.socketio.SocketIOServer;
import com.corundumstudio.socketio.annotation.OnConnect;
import com.corundumstudio.socketio.annotation.OnDisconnect;
import com.corundumstudio.socketio.annotation.OnEvent;

public class BenchPressService implements ServerCallback {
    private static final Logger LOG = Logger.getLogger(BenchPressService.class);
    
    private static final int DEFAULT_THROUGHPUT = 200;
    private static Timer timer = null;

    private volatile int targetThroughput = DEFAULT_THROUGHPUT;
    private volatile int actualThroughput = DEFAULT_THROUGHPUT;
    
    public static boolean GAME_BEHAVIOR = false;
    public static boolean DONE = true;
    private volatile GameThread gthread = null;
    private SocketIOServer server;
    
    private class SendThroughputTask extends TimerTask {

        @Override
        public void run() {
            server.getBroadcastOperations().sendEvent(
                    "height", Integer.valueOf(actualThroughput).toString());
        }
    }
    
    public BenchPressService(SocketIOServer server) {
        this.server = server;
        GAME_BEHAVIOR = true;
    }

    @Override
    public void ready() {
        System.out.println("Backend ready... sending ready response to client\n");
        server.getBroadcastOperations().sendEvent("setup", "ready");
    }

    @Override
    public void updateActualThroughput(int actualThroughput) {
        this.actualThroughput = actualThroughput;
    }

    @Override
    public void updateTargetThroughput(int targetThroughput) {
        this.targetThroughput = targetThroughput;
    }
    
    @Override
    public int getTargetThroughput() {
        return targetThroughput;
    }
    
    @OnEvent("setup")
    public void setupHandler(SocketIOClient client, DBConfig data, AckRequest ackRequest) {
        System.out.println("Received game configuration from client:\n"
                + data.toString() + "\n");
        if (!data.isValid()) {
            data.setDefaults();
            System.out.println("Unrecognized config: " + data.getDbms() + ", " + 
                    data.getBenchmark() + ". Starting game with default config: MySQL, YCSB");
        }
        // Start the game thread - this executes the benchmark
        gthread = new GameThread(data);
        gthread.registerCallback(this);
        DONE = false;
        gthread.start();

        // Setup throughput timer to send update every X ms
        final TimerTask task = new SendThroughputTask();
        timer = new Timer();
        timer.schedule(task, 0, 50);
    }
    
    @OnEvent("height")
    public void throughputHandler(SocketIOClient client, String data, AckRequest ackRequest) {
        System.out.println("Received new height from client: " + data);    
        targetThroughput = Integer.valueOf(data);
    }
    
    @OnEvent("gameover")
    public void gameoverHandler(SocketIOClient client, String data, AckRequest ackRequest) {
        targetThroughput = DEFAULT_THROUGHPUT;
        if (data.equals("restart")) {
            // Client wants to restart level with same db and benchmark.
            // Just reset height to default instead of restarting db.
            System.out.println("Restarting game\n");
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } else if (data.equals("menu")) {
            System.out.println("Returning to menu (stop db)\n");
            if (timer != null) {
                timer.cancel();
                timer.purge();
                timer = null;
            }
            DONE = true;
            
        } else {
            System.out.println("Unrecognized gameover option: " + data + "\n");
        }
    }

    @OnConnect
    public void onConnectHandler(SocketIOClient client) {
        System.out.println("Connected to client" +
                client.getSessionId().toString() + "\n");
    }

    @OnDisconnect
    public void onDisconnectHandler(SocketIOClient client) {
        System.out.println("Disconnected from client" +
                client.getSessionId().toString() + "\n");
    }

}
