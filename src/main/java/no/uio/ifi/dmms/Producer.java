package no.uio.ifi.dmms;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.*;


public class Producer extends Thread {
    private int remotePort;
    private ServerSocket serverSocket;
    private Socket clientSocket;

    public Producer() {
        remotePort = 1080;
    }

    public void connect() {
        System.out.println("Producer waiting for server to connect..");
        try {
            serverSocket = new ServerSocket(remotePort);
            clientSocket = serverSocket.accept();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(" -- Connected --");
    }

    public void send(String message) {
        PrintWriter out = null;
        try {
            out = new PrintWriter(clientSocket.getOutputStream(), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        out.println(message);
    }

    public void run() {
        connect();

        int i = 0;
        while (true) {
            String eventString = System.nanoTime()+","+i++;
            send(eventString);
            //System.out.println("Sent "+eventString);
            try {
                sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
