package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Node extends AbstractActor {
    final int ringNum;
    final int W_QUORUM;
    final int R_QUORUM;
    final int N_REPLICAS;
    final FiniteDuration TIMEOUT;
    SortedMap<Integer,ActorRef> group;
    Map<Integer,Element> data;
    Map<Integer,Map<NodeGet,GetRequestTracker>> coordGetList;
    Map<Integer,UpdateRequestTracker> coordUpdateList;
    Map<Integer,NodeUpdate> nodeUpdateList;

    //---------------------------------------------------------------------------------
    // Constructors
    public Node (int ringNum, int replFactor, int r, int w, int timeout) {
        this.ringNum = ringNum;
        N_REPLICAS = replFactor;
        R_QUORUM = r;
        W_QUORUM = w;
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
        Element elem;
        public GetRequestTracker(){
            nSuccess = 0;
            nFail = 0;
            nonExistent = 0;
            elem = null;
        }
    }

    static public class UpdateRequestTracker {
        String data ;
        NodeUpdate nu;
        int version;
        int nSuccess;
        int nFail;
        public UpdateRequestTracker(String data, NodeUpdate nu){
            this.data = data;
            this.nu = nu;
            version = 0;
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

    static public class Timeout implements Serializable {
        Serializable m;
        public Timeout(Serializable m){ this.m = m; }
    }

    static public class NodeGet implements Serializable{
        int elemKey;
        ActorRef client;
        public NodeGet(int elemKey, ActorRef client) {
            this.elemKey = elemKey;
            this.client = client;
        }
    }

    static public class NodeUpdate implements Serializable{
        int elemKey;
        ActorRef client;
        public NodeUpdate(int elemKey, ActorRef client) {
            this.elemKey = elemKey;
            this.client = client;
        }
    }

    static public class OkGet implements Serializable{
        NodeGet req;
        Element elem;
        public OkGet(NodeGet req, Element elem) {
            this.req = req;
            this.elem = elem;
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
        Element elem;
        public CommitUpdate(NodeUpdate req, Element el) {
            this.req = req;
            this.elem = el;
        }
    }

    static public class AbortUpdate implements Serializable {
        NodeUpdate req;
        public AbortUpdate(NodeUpdate req) { this.req = req; }
    }

    static public class Print implements Serializable {}

    //---------------------------------------------------------------------------------
    // Helper functions

    private void transmitWithRingNumber(int node, Serializable mess) {
        if(node == ringNum) {
            getSelf().tell(mess,getSelf());
        } else {
            int delay = (int) (Math.random()*10) + 10;
            getContext().system().scheduler().scheduleOnce(
                    Duration.create(delay,TimeUnit.MILLISECONDS),
                    group.get(node),
                    mess,
                    getContext().system().dispatcher(),
                    getSelf()
            );
        }
    }

    private void transmitWithActorRef(ActorRef ar, Serializable mess) {
        if(ar == getSelf()) {
            getSelf().tell(mess,getSelf());
        } else {
            int delay = (int) (Math.random()*10) + 10;
            getContext().system().scheduler().scheduleOnce(
                    Duration.create(delay,TimeUnit.MILLISECONDS),
                    ar,
                    mess,
                    getContext().system().dispatcher(),
                    getSelf()
            );
        }
    }

    private void sendToNodes(int elemKey, Serializable mess) {
        int toContact = N_REPLICAS;
        for(int node : group.tailMap(elemKey).keySet()){
            transmitWithRingNumber(node,mess);
            toContact--;
            if(toContact == 0){ break; }
        }
        if(toContact > 0){
            for(int node : group.keySet()){
                transmitWithRingNumber(node,mess);
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

    private void onGet (Get getMess) {
        NodeGet ng = new NodeGet(getMess.elemKey, getSender());
        coordGetList.putIfAbsent(getMess.elemKey,new HashMap<>());

        if(coordUpdateList.containsKey(getMess.elemKey)){
            getSender().tell(new Client.Result(
                    "Get " + getMess.elemKey +
                    ": The coordinator is already processing an update on the same key"
            ),getSelf());
            return;
        }

        coordGetList.get(getMess.elemKey).put(ng,new GetRequestTracker());

        sendToNodes(getMess.elemKey,ng);
        getContext().system().scheduler().scheduleOnce(
                TIMEOUT,
                getSelf(),
                new Timeout(ng),
                getContext().system().dispatcher(),
                getSelf()
        );
    }

    private void onUpdate (Update updMess) {
        NodeUpdate nu = new NodeUpdate(updMess.elemKey,getSender());

        if(coordUpdateList.putIfAbsent(
                updMess.elemKey, new UpdateRequestTracker(updMess.data,nu)) != null)
        {
            getSender().tell(new Client.Result(
                    "Update " + updMess.elemKey +
                    ": The coordinator is already processing an update on the same key"
            ), getSender());
            return;
        }
        sendToNodes(updMess.elemKey,nu);
        getContext().system().scheduler().scheduleOnce(
                TIMEOUT,
                getSelf(),
                new Timeout(nu),
                getContext().system().dispatcher(),
                getSelf()
        );
    }

    private void onTimeout (Timeout timeout){
        if(timeout.m instanceof NodeGet ng){
            GetRequestTracker t = coordGetList.get(ng.elemKey).remove(ng);
            if(t == null) return;
            transmitWithActorRef(
                    ng.client,
                    new Client.Result("Get " + ng.elemKey + ": Timed out. Cannot reach quorum")
            );
        } else if(timeout.m instanceof NodeUpdate nu){
            UpdateRequestTracker tracker = coordUpdateList.get(nu.elemKey);

            if(tracker == null || !tracker.nu.equals(nu)) return;

            coordUpdateList.remove(nu.elemKey);
            transmitWithActorRef(
                    nu.client,
                    new Client.Result("Update " + nu.elemKey
                            + ": Timed out. Cannot reach quorum")
            );
            sendToNodes(nu.elemKey, new AbortUpdate(nu));
        }
    }

    private void onNodeGet (NodeGet ng) {
        if(nodeUpdateList.containsKey(ng.elemKey)){
            transmitWithActorRef(getSender(), new BadGet(ng));
        } else {
            transmitWithActorRef(getSender(),new OkGet(ng,data.get(ng.elemKey)));
        }
    }

    private void onNodeUpdate (NodeUpdate nu) {
        if(nodeUpdateList.putIfAbsent(nu.elemKey,nu) != null){
            transmitWithActorRef(getSender(),new BadUpdate(nu));
            return;
        }
        Element e = data.get(nu.elemKey);
        int ver = e == null ? 0 : e.version;
        transmitWithActorRef(getSender(),new OkUpdate(nu,ver));
    }

    private void onOkGet (OkGet okMess) {
        GetRequestTracker tracker = coordGetList.get(okMess.req.elemKey).get(okMess.req);
        if(tracker == null) return;

        if(okMess.elem == null) {
            tracker.nonExistent++;
            if(tracker.nonExistent >= R_QUORUM){
                coordGetList.get(okMess.req.elemKey).remove(okMess.req);
                transmitWithActorRef(
                        okMess.req.client,
                        new Client.Result(
                                "Get " + okMess.req.elemKey + ": No element with such key exists"
                        )
                );
            }
        }
        else{
            tracker.nSuccess++;
            if(tracker.elem == null) tracker.elem = okMess.elem;
            else if(tracker.elem.version < okMess.elem.version){
                tracker.elem = okMess.elem;
            }
            if(tracker.nonExistent + tracker.nSuccess >= R_QUORUM){
                coordGetList.get(okMess.req.elemKey).remove(okMess.req);
                transmitWithActorRef(
                        okMess.req.client,
                        new Client.Result("Get " + okMess.req.elemKey + ": " + okMess.elem)
                );
            }
        }
    }

    private void onBadGet (BadGet badMess) {
        GetRequestTracker tracker = coordGetList.get(badMess.req.elemKey).get(badMess.req);
        if(tracker == null) return;

        tracker.nFail++;
        if(tracker.nFail >= R_QUORUM){
            coordGetList.get(badMess.req.elemKey).remove(badMess.req);
            transmitWithActorRef(
                    badMess.req.client,
                    new Client.Result("Get " + badMess.req.elemKey +
                            ": Can't get value because an update is still running")
            );
        }
    }

    private void onOkUpdate (OkUpdate okMess) {
        UpdateRequestTracker tracker = coordUpdateList.get(okMess.req.elemKey);

        if(tracker == null || !tracker.nu.equals(okMess.req)) return;

        tracker.nSuccess++;
        tracker.version = Math.max(tracker.version, okMess.version);
        if(tracker.nSuccess >= W_QUORUM){
            coordUpdateList.remove(okMess.req.elemKey);
            transmitWithActorRef(
                    okMess.req.client,
                    new Client.Result("Update " + okMess.req.elemKey
                    + ": Successful update with new version " + (tracker.version + 1))
            );
            sendToNodes(okMess.req.elemKey,new CommitUpdate(
                    okMess.req,
                    new Element(okMess.req.elemKey,tracker.data,tracker.version + 1)
            ));
        }
    }

    private void onBadUpdate (BadUpdate badMess) {
        UpdateRequestTracker tracker = coordUpdateList.get(badMess.req.elemKey);

        if(tracker == null || !tracker.nu.equals(badMess.req)) return;

        tracker.nFail++;
        if(tracker.nFail >= W_QUORUM){
            coordUpdateList.remove(badMess.req.elemKey);
            transmitWithActorRef(
                    badMess.req.client,
                    new Client.Result("Update " + badMess.req.elemKey
                    + ": Failure as there is a concurrent write running")
            );
            sendToNodes(badMess.req.elemKey, new AbortUpdate(badMess.req));
        }
    }

    private void onCommitUpdate (CommitUpdate commitMess) {
        data.put(commitMess.req.elemKey,commitMess.elem);
        nodeUpdateList.remove(commitMess.req.elemKey,commitMess.req);
    }

    private void onAbortUpdate (AbortUpdate abortMess) {
        nodeUpdateList.remove(abortMess.req.elemKey,abortMess.req);
    }

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
                .match(AbortUpdate.class,this::onAbortUpdate)
                .match(Print.class,this::onPrint)
                .build();
    }

    public AbstractActor.Receive crashed() {
        return receiveBuilder().build();
    }
}
