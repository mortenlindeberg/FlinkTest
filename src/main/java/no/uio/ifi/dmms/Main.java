package no.uio.ifi.dmms;

public class Main {

    public static void main(String[] args) throws Exception {
        GPXProducer producer = new GPXProducer();
        Consumer consumer = new Consumer();
        consumer.start();
        producer.start();

        boolean sending = true;
        while(sending) {
            Thread.sleep(1000);
            if (!producer.isAlive())
                sending = false;
        }
        Thread.sleep(5000);
        System.exit(0);
    }

}
