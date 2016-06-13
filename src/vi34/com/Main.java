package vi34.com;

/**
 * Created by vi34 on 11/06/16.
 */
public class Main {
    static int n = 3;
    static Thread[] replicas;
    public static void main(String[] args) {
        if (args.length == 0) {
            try {
               replicas = new Thread[n];
                for (int i = 0; i < n; ++i) {
                    replicas[i] = new Thread(new Replica(i + 1));
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
        System.out.println(proxy.sendRequest("get x"));
        System.out.println(proxy.sendRequest("ping"));
        System.out.println(proxy.sendRequest("set x 10"));
        System.out.println(proxy.sendRequest("get x"));

    }

}
