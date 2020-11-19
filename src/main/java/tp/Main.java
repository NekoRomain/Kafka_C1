package tp;


import tp.consumer.ConsumerUn;
import tp.database.Database;

import java.sql.SQLException;
import java.util.Timer;


public class Main {

    public Main() throws InterruptedException, SQLException {
        Database d = Database.getInstance();
        d.createTable();
        ConsumerUn c = new ConsumerUn();
        Thread t = new Thread(c);
        t.run();


        while (true) {

        }
    }

    public static void main(String[] args) throws InterruptedException, SQLException {
//            Logger.getLogger("org").setLevel(Level.);
//            Logger.getLogger("akka").setLevel(Level.);
//            Logger.getLogger("main").setLevel(Level.);
        Main main = new Main();
    }
}