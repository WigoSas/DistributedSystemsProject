package it.unitn.ds1;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.io.IOException;
import java.util.*;

public class Project {
  //final static int N_NODES = 10;
  final static int N_CLIENTS = 5;
  final static int REPLICATION_FACTOR = 5;
  final static int READ_QUORUM = 3;
  final static int WRITE_QUORUM = 3;
  final static int TIMEOUT = 1000;

  public static void main(String[] args) throws IOException, InterruptedException {
    // Create the actor system
    final ActorSystem system = ActorSystem.create("banksystem");
    SortedMap<Integer,ActorRef> group = new TreeMap<>();

    for(int i = 10; i<=100; i+=10){
      ActorRef ar = system.actorOf(Node.props(i,REPLICATION_FACTOR,READ_QUORUM,WRITE_QUORUM,TIMEOUT));
      group.put(i,ar);
    }

    Node.Initialisation init = new Node.Initialisation(group);
    for(ActorRef a : group.values()){
      a.tell(init,ActorRef.noSender());
    }

    Thread.sleep(100);
    System.out.println("NODES INITIALISATION DONE. PRESS TO CONTINUE");
    System.in.read();

    List<ActorRef> clients = new ArrayList<>();
    for(int i=0; i<N_CLIENTS; i++){
      clients.add(system.actorOf(Client.props(group,"CLIENT"+i)));
    }

    Thread.sleep(100);
    System.out.println("\nMULTIPLE CONCURRENT UPDATES ON DIFFERENT KEYS. PRESS TO CONTINUE");
    System.in.read();

    clients.get(0).tell(new Client.AskToUpdate(60,13,"water"),ActorRef.noSender());
    clients.get(1).tell(new Client.AskToUpdate(60,37,"hydrogen"),ActorRef.noSender());
    clients.get(2).tell(new Client.AskToUpdate(30,53,"iron"),ActorRef.noSender());
    clients.get(3).tell(new Client.AskToUpdate(100,67,"ice"),ActorRef.noSender());
    clients.get(4).tell(new Client.AskToUpdate(50,83,"granite"),ActorRef.noSender());

    Thread.sleep(1500);
    System.out.println("\nPRESS TO SEE THE RESULT");
    System.in.read();

    Node.Print p = new Node.Print();
    for(ActorRef a : group.values()){
      a.tell(p,ActorRef.noSender());
    }

    Thread.sleep(100);
    System.out.println("\nMULTIPLE CONCURRENT GETS, ALSO ON THE SAME KEY.PRESS TO CONTINUE");
    System.in.read();

    clients.get(0).tell(new Client.AskToGet(40,53),ActorRef.noSender());
    clients.get(1).tell(new Client.AskToGet(40,53),ActorRef.noSender());
    clients.get(2).tell(new Client.AskToGet(70,53),ActorRef.noSender());
    clients.get(3).tell(new Client.AskToGet(40,53),ActorRef.noSender());
    clients.get(4).tell(new Client.AskToGet(20,53),ActorRef.noSender());

    Thread.sleep(1500);
    System.out.println("\nMULTIPLE WRITES ON THE SAME KEY. PRESS TO CONTINUE");
    System.in.read();

    clients.get(0).tell(new Client.AskToUpdate(60,37,"hydrogen0"),ActorRef.noSender());
    //Thread.sleep(2);
    clients.get(1).tell(new Client.AskToUpdate(60,37,"hydrogen1"),ActorRef.noSender());
    clients.get(2).tell(new Client.AskToUpdate(30,37,"hydrogen2"),ActorRef.noSender());
    clients.get(3).tell(new Client.AskToUpdate(100,37,"hydrogen3"),ActorRef.noSender());
    clients.get(4).tell(new Client.AskToUpdate(50,37,"hydrogen4"),ActorRef.noSender());

    Thread.sleep(1500);
    System.out.println("\nPRESS TO SEE THE RESULT");
    System.in.read();

    for(ActorRef a : group.values()){
      a.tell(p,ActorRef.noSender());
    }

    Thread.sleep(100);
    System.out.println("\nONE UPDATE MULTIPLE GETS ON THE SAME KEY. PRESS TO CONTINUE");
    System.in.read();

    clients.get(0).tell(new Client.AskToGet(40,83),ActorRef.noSender());
    clients.get(1).tell(new Client.AskToUpdate(90,83,"granite+"),ActorRef.noSender());
    clients.get(2).tell(new Client.AskToGet(20,83),ActorRef.noSender());
    clients.get(3).tell(new Client.AskToGet(40,83),ActorRef.noSender());
    Thread.sleep(80);
    clients.get(4).tell(new Client.AskToGet(70,83),ActorRef.noSender());

    Thread.sleep(1500);
    System.out.println("\nPRESS TO SEE THE RESULT");
    System.in.read();

    for(ActorRef a : group.values()){
      a.tell(p,ActorRef.noSender());
    }

    Thread.sleep(100);
    System.out.println("\nPRESS TO END");
    System.in.read();

    system.terminate();
  }


}
