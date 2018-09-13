package no.uio.ifi.dmms;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class GPXProducer extends Thread {
    private String filename;
    private int remotePort;
    private int sleepTime;
    private ServerSocket serverSocket;
    private Socket clientSocket;

    public GPXProducer(int sleepTime) {
        this.filename = "trace.gpx";
        this.remotePort = 1080;
        this.sleepTime = sleepTime;
    }

    public void connect() {
        System.out.println(" -> Producer thread waiting for server to connect..");
        try {
            serverSocket = new ServerSocket(remotePort);
            clientSocket = serverSocket.accept();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(" -> Producer is now connected to flink node");
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

        File inputFile = new File(filename);
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = null;
        try {
            dBuilder = dbFactory.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        }
        Document doc = null;
        try {
            doc = dBuilder.parse(inputFile);
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        doc.getDocumentElement().normalize();


        NodeList trackptList = doc.getElementsByTagName("trkpt");
        Node currentTrackPoint;
        int i;
        for (i = 0; i < trackptList.getLength(); i++) {
            currentTrackPoint = trackptList.item(i);
            if (currentTrackPoint.getNodeType() == Node.ELEMENT_NODE) {
                GpxTuple tuple = new GpxTuple();
                tuple.setLat(currentTrackPoint.getAttributes().getNamedItem("lat").getTextContent());
                tuple.setLon(currentTrackPoint.getAttributes().getNamedItem("lon").getTextContent());
                String time = "";
                String power = "";
                String heartate = "";
                String cad = "";

                NodeList pointElements = currentTrackPoint.getChildNodes();

                for (int j = 0; j < pointElements.getLength(); j++) {
                    Node currentPointElement = pointElements.item(j);
                    if (currentPointElement.getNodeName().equals("time"))
                        tuple.setTime(currentPointElement.getTextContent());
                    else if (currentPointElement.getNodeName().equals("extensions")) {
                        NodeList extensionElements = currentPointElement.getChildNodes();

                        for (int k = 0; k < extensionElements.getLength(); k++) {
                            Node currentExtensionElement = extensionElements.item(k);
                            if (currentExtensionElement.getNodeName().equals("power"))
                                tuple.setPower(currentExtensionElement.getTextContent());
                            else if (currentExtensionElement.getNodeName().equals("gpxtpx:TrackPointExtension")) {
                                NodeList tpExtensionElements = currentExtensionElement.getChildNodes();

                                for (int l = 0; l < tpExtensionElements.getLength(); l++) {
                                    Node currentTpExtensionElement = tpExtensionElements.item(l);
                                    if (currentTpExtensionElement.getNodeName().equals("gpxtpx:atemp"))
                                        tuple.setTemp(currentTpExtensionElement.getTextContent());
                                    else if (currentTpExtensionElement.getNodeName().equals("gpxtpx:hr"))
                                        tuple.setHeartrate(currentTpExtensionElement.getTextContent());
                                    else if (currentTpExtensionElement.getNodeName().equals("gpxtpx:cad"))
                                        tuple.setCadence(currentTpExtensionElement.getTextContent());
                                }
                            }
                        }
                    }

                }
                send(tuple.getPosixString()+","+tuple.getLat()+","+tuple.getLon()+","+tuple.getPower()+","+tuple.getCadence()+","+tuple.getHeartrate()+","+tuple.getTemp());
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println(" -- Finished Sending "+i+" tuples--");
    }
}