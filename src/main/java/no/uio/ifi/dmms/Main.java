package no.uio.ifi.dmms;

public class Main {

    public static void main(String[] args) {
        GPXProducer producer = new GPXProducer();
        //Producer producer = new Producer();
        Consumer consumer = new Consumer();
        consumer.start();
        producer.start();
    }
}
