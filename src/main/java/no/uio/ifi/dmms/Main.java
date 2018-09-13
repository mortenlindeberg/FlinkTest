package no.uio.ifi.dmms;

public class Main {

    public static void main(String[] args) throws Exception {
        //GPXProducer producer = new GPXProducer();
        GpxProducerCurrentTime producer = new GpxProducerCurrentTime(10);
        //Consumer consumer = new Consumer();
        GpxHeartPowerPattern consumer = new GpxHeartPowerPattern();

        consumer.start();
        producer.start();

        boolean sending = true;
        while(sending) {
            Thread.sleep(1000);
            if (!producer.isAlive())
                sending = false;
        }
        Thread.sleep(30000);
        System.exit(0);
    }

}
