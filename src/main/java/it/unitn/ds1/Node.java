package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Node extends AbstractActor {
    final int ringNum;
    final int W;
    final int R;
    final int N;
    final Duration TIMEOUT;
    SortedMap<Integer,ActorRef> group;
    Map<Integer,Element> data;
    Map<Integer,Map<NodeGet,GetRequestTracker>> coordGetList;
    Map<Integer,UpdateRequestTracker> coordUpdateList;
    Map<Integer,NodeUpdate> nodeUpdateList;

    //---------------------------------------------------------------------------------
    // Constructors
    public Node (int ringNum, int replFactor, int r, int w, int timeout) {
        this.ringNum = ringNum;
        N = replFactor;
        R = r;
        W = w;
        TIMEOUT = Duration.create(timeout,TimeUnit.MILLISECONDS);
        group = new TreeMap<>();
        data = new HashMap<>();
        coordGetList = new HashMap<>();
        coordUpdateList = new HashMap<>();
        nodeUpdateList = new HashMap<>();
    }

    static public Props props(int ringNum, int replFactor, int r, int w, int timeout) {
        return Props.create(Node.class, () -> new Node(ringNum,replFactor,r,w,timeout));
    }

    //---------------------------------------------------------------------------------
    // Additional helper classes
    static public class GetRequestTracker {
        int nSuccess;
        int nFail;
        int nonExistent;
        Element el;
        public GetRequestTracker(){
            nSuccess = 0;
            nFail = 0;
            nonExistent = 0;
            el = null;
        }
    }

    static public class UpdateRequestTracker {
        NodeUpdate m;
        int nSuccess;
        int nFail;
        public UpdateRequestTracker(NodeUpdate m){
            this.m = m;
            nSuccess = 0;
            nFail = 0;
        }
    }

    //---------------------------------------------------------------------------------
    // Messages

    static public class Initialisation implements Serializable {
        SortedMap<Integer,ActorRef> group;
        public Initialisation (SortedMap<Integer,ActorRef> group) {
            this.group = group;
        }
    }

    static public class Get implements Serializable {
        int elemKey;
        public Get (int elemKey) { this.elemKey = elemKey; }
    }

    static public class Update implements Serializable {
        int elemKey;
        String data;
        public Update (int elemKey, String data) {
            this.elemKey = elemKey;
            this.data = data;
        }
    }

    static public class Timeout implements Serializable {}

    static public class NodeGet implements Serializable{
        int id;
        int elemKey;
        ActorRef client;
        public NodeGet(int id, int elemKey, ActorRef client) {
            this.id = id;
            this.elemKey = elemKey;
            this.client = client;
        }
    }

    static public class NodeUpdate implements Serializable{
        int id;
        int elemKey;
        ActorRef client;
        public NodeUpdate(int id, int elemKey, ActorRef client) {
            this.id = id;
            this.elemKey = elemKey;
            this.client = client;
        }
    }

    static public class OkGet implements Serializable{
        NodeGet req;
        Element el;
        public OkGet(NodeGet req, Element el) {
            this.req = req;
            this.el = el;
        }
    }

    static public class BadGet implements Serializable {
        NodeGet req;
        public BadGet(NodeGet req) { this.req = req; }
    }

    static public class OkUpdate implements Serializable {
        NodeUpdate req;
        int version;
        public OkUpdate(NodeUpdate req, int version) {
            this.req = req;
            this.version = version;
        }
    }

    static public class BadUpdate implements Serializable {
        NodeUpdate req;
        public BadUpdate(NodeUpdate req) { this.req = req; }
    }

    static public class CommitUpdate implements Serializable {
        NodeUpdate req;
        Element element;
        public CommitUpdate(NodeUpdate req, Element el) {
            this.req = req;
            this.element = el;
        }
    }

    static public class AbortUpdate implements Serializable {
        NodeUpdate req;
        public AbortUpdate(NodeUpdate req) { this.req = req; }
    }

    static public class Print implements Serializable {}

    //---------------------------------------------------------------------------------
    // Helper functions

    private void transmitWithRingNumber(int node, Serializable el) {
        if(node == ringNum) {
            getSelf().tell(el,getSelf());
        } else {
            int delay = (int) (Math.random()*10) + 10;
            getContext().system().scheduler().scheduleOnce(
                    Duration.create(delay,TimeUnit.MILLISECONDS),
                    group.get(node),
                    el,
                    getContext().system().dispatcher(),
                    getSelf()
            );
        }
    }

    private void transmitWithActorRef(ActorRef ar, Serializable el) {
        if(ar == getSelf()) {
            getSelf().tell(el,getSelf());
        } else {
            int delay = (int) (Math.random()*10) + 10;
            getContext().system().scheduler().scheduleOnce(
                    Duration.create(delay,TimeUnit.MILLISECONDS),
                    ar,
                    el,
                    getContext().system().dispatcher(),
                    getSelf()
            );
        }
    }

    private void sendToNodes(int elemKey, Serializable el) {
        int toContact = N;
        for(int node : group.tailMap(elemKey).keySet()){
            System.out.println(node);
            transmitWithRingNumber(node,el);
            toContact--;
            if(toContact == 0){ break; }
        }
        if(toContact > 0){
            for(int node : group.keySet()){
                System.out.println(node);
                transmitWithRingNumber(node,el);
                toContact--;
                if(toContact == 0){ break; }
            }
        }
    }

    //---------------------------------------------------------------------------------
    // Behaviour
    private void onInitialisation (Initialisation el){
        group.putAll(el.group);
    }

    private void onGet (Get el) {
        NodeGet ng = new NodeGet(0, el.elemKey, getSender());
        coordGetList.putIfAbsent(el.elemKey,new HashMap<>());

        if(coordUpdateList.containsKey(el.elemKey)){
            getSender().tell(new Client.Result(
                    "Get " + el.elemKey +
                    ": Failure - The coordinator is already processing an update"
            ),getSelf());
            return;
        }

        coordGetList.get(el.elemKey).put(ng,new GetRequestTracker());

        sendToNodes(el.elemKey,ng);
    }

    private void onUpdate (Update el) {}

    private void onTimeout (Timeout el){}

    private void onNodeGet (NodeGet el) {
        if(nodeUpdateList.containsKey(el.elemKey)){
            transmitWithActorRef(getSender(), new BadGet(el));
        } else {
            transmitWithActorRef(getSender(),new OkGet(el,data.get(el.elemKey)));
        }
    }

    private void onNodeUpdate (NodeUpdate el) {}

    private void onOkGet (OkGet el) {
        GetRequestTracker tracker = coordGetList.get(el.req.elemKey).get(el.req);
        if(tracker == null) return;

        if(el.el == null) {
            tracker.nonExistent++;
            if(tracker.nonExistent > R){
                transmitWithActorRef(
                        el.req.client,
                        new Client.Result("Get: No element with key " + el.req.elemKey + " exists")
                );
                coordGetList.get(el.req.elemKey).remove(el.req);
            }
        }
        else{
            tracker.nSuccess++;
            if(tracker.el == null) tracker.el = el.el;
            else if(tracker.el.version < el.el.version){
                tracker.el = el.el;
            }
            if(tracker.nonExistent + tracker.nSuccess > R){
                transmitWithActorRef(
                        el.req.client,
                        new Client.Result("Get: " + el.el)
                );
                coordGetList.get(el.req.elemKey).remove(el.req);
            }
        }
    }

    private void onBadGet (BadGet el) {}

    private void onOkUpdate (OkUpdate el) {}

    private void onBadUpdate (BadUpdate el) {}

    private void onCommitUpdate (CommitUpdate el) {}

    private void onAbortupdate (AbortUpdate el) {}

    private void onPrint (Print el) {
        System.out.println("Node" + ringNum + ": " + data);
    }


    //
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Initialisation.class,this::onInitialisation)
                .match(Get.class,this::onGet)
                .match(Update.class,this::onUpdate)
                .match(Timeout.class,this::onTimeout)
                .match(NodeGet.class,this::onNodeGet)
                .match(NodeUpdate.class,this::onNodeUpdate)
                .match(OkGet.class,this::onOkGet)
                .match(BadGet.class,this::onBadGet)
                .match(OkUpdate.class,this::onOkUpdate)
                .match(BadUpdate.class,this::onBadUpdate)
                .match(CommitUpdate.class,this::onCommitUpdate)
                .match(AbortUpdate.class,this::onAbortupdate)
                .match(Print.class,this::onPrint)
                .build();
    }
}
