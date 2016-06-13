package vi34.com;

/**
 * Created by vi34 on 11/06/16.
 */
public class Main {
    static int f = 3;
    static Thread[] replicas;
    public static void main(String[] args) {
        if (args.length == 0) {
            try {
               replicas = new Thread[f];
                for (int i = 0; i < f; ++i) {
                    replicas[i] = new Thread(new Replica(i));
                    replicas[i].start();
                }
                Thread.sleep(100);
                Proxy proxy = new Proxy();
                client(proxy);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                for (Thread replica : replicas) {
                    //replica.interrupt();
                }
            }
        }
    }

    static void client(Proxy proxy) {
        System.out.println(proxy.sendRequest("set k1 1"));

    }

}
