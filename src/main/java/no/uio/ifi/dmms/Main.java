package no.uio.ifi.dmms;

public class Main {

    public static void main(String[] args) {
        Producer producer = new Producer();
        Consumer consumer = new Consumer();
        consumer.start();
        producer.start();
    }
}
