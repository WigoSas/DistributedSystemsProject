package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.SortedMap;
import java.util.TreeMap;

public class Client extends AbstractActor {
    private SortedMap<Integer, ActorRef> map;
    String name;
    boolean isBusy;

    // Constructors
    public Client(SortedMap<Integer,ActorRef> list, String name){
        map = new TreeMap<>(list);
        this.name = name;
        isBusy = false;
    }

    static public Props props(SortedMap<Integer,ActorRef> list, String name){
        return Props.create(Client.class, () -> new Client(list,name));
    }

    // Messages
    static public class AskToGet implements Serializable {
        Integer node;
        Integer elemKey;
        public AskToGet(Integer node, Integer elemKey){
            this.node = node;
            this.elemKey = elemKey;
        }
    }

    static public class AskToUpdate implements Serializable {
        Integer node;
        Integer elemKey;
        String value;
        public AskToUpdate(Integer node, Integer elemKey, String value) {
            this.node = node;
            this.elemKey = elemKey;
            this.value = value;
        }
    }

    static public class Result implements Serializable {
        String res;
        public Result (String res) { this.res = res; }
    }

    // behaviour
    private void onAskToGet(AskToGet ask) {
        if(isBusy) {
            System.out.println(name + " : Already occupied with another command");
            return;
        }
        isBusy = true;
        ActorRef node = map.get(ask.node);
        if(node == null){
            System.out.println(name + " : No node with key " + ask.node);
            isBusy = false;
        } else {
            node.tell(new Node.Get(ask.elemKey), getSelf());
        }
    }

    private void onAskToUpdate(AskToUpdate ask) {
        if(isBusy) {
            System.out.println(name + " : Already occupied with another command");
            return;
        }
        isBusy = true;
        ActorRef node = map.get(ask.node);
        if(node == null){
            System.out.println(name + " : No node with key " + ask.node);
            isBusy = false;
        } //else {}
    }

    private void onResult (Result res) {
        System.out.println(name + " " + res.res);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(AskToGet.class,this::onAskToGet)
                .match(AskToUpdate.class,this::onAskToUpdate)
                .match(Result.class,this::onResult)
                .build();
    }
}
