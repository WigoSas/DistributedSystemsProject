package it.unitn.ds1;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import java.io.IOException;
import java.util.*;

public class Project {
  final static int N_NODES = 10;
  final static int N_CLIENTS = 5;
  final static int REPLICATION_FACTOR = 5;
  final static int READ_QUORUM = 3;
  final static int WRITE_QUORUM = 3;
  final static int TIMEOUT = 1000;

  public static void main(String[] args) throws IOException {
    // Create the actor system
    final ActorSystem system = ActorSystem.create("banksystem");

    Set<Node.NodeGet> set = new HashSet<>();

    Node.NodeGet n = new Node.NodeGet(2,23,ActorRef.noSender());
    set.add(n);
    boolean res = set.add(new Node.NodeGet(2,23,ActorRef.noSender()));
    System.out.println(res);

    Set<Node.NodeGet> set2 = new HashSet<>();
    set2.addAll(set);

    String x = set + "\n" + set2 + "\n" + set.equals(set2);
    System.out.println(x);

    set.remove(n);
    System.out.println(x);

    SortedMap<Integer,ActorRef> group = new TreeMap<>();
    for(int i = 10; i<=100; i+=10){
      ActorRef ar = system.actorOf(Node.props(i,REPLICATION_FACTOR,READ_QUORUM,WRITE_QUORUM,TIMEOUT));
      group.put(i,ar);
    }

    Node.Initialisation init = new Node.Initialisation(group);
    for(ActorRef a : group.values()){
      a.tell(init,ActorRef.noSender());
    }

    System.out.println("PRESS TO CONTINUE");
    System.in.read();

    Node.Print p = new Node.Print();
    for(ActorRef a : group.values()){
      a.tell(p,ActorRef.noSender());
    }

    ActorRef client = system.actorOf(Client.props(group,"Client"));


    group.get(group.firstKey()).tell(new Node.Get(30),client);

    System.out.println("PRESS TO CONTINUE");
    System.in.read();

    group.get(50).tell(new Node.Get(190),client);

    System.out.println("PRESS TO END");
    System.in.read();


    // Send join messages to the banks to inform them of the whole group
    //JoinGroupMsg start = new JoinGroupMsg(group);
    //for (ActorRef peer: group) {
    //  peer.tell(start, ActorRef.noSender());
    //}

    system.terminate();
  }
}
