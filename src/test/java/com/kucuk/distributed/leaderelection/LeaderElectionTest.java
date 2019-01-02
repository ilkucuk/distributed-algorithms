package com.kucuk.distributed.leaderelection;

import org.apache.log4j.BasicConfigurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LeaderElectionTest {

    private final Random random = new Random();

    @BeforeClass
    public static void setUp(){
        BasicConfigurator.configure();
    }

    private List<Node> createNodes(int count, int threadPoolSize, int delayMiliseconds, int dropPercentage) {
        List<Node> nodes = new ArrayList<>();
        for(int i=0; i<count; i++) {
            nodes.add(new ElectionCoordinator(random.nextLong(), threadPoolSize));
        }

        List<Node> connections = nodes.stream()
                .map( node -> new NodeConnection(node, delayMiliseconds, dropPercentage))
                .collect(Collectors.toList());

        nodes.forEach(node -> ((ElectionCoordinator)node).addAll(connections));
        return nodes;
    }

    @Test
    public void electLeaderWhileHighestPriority() throws InterruptedException {
        List<Node> nodes = createNodes(3, 3,0,0);
        nodes.forEach(node -> ((ElectionCoordinator)node).start(100, 100, TimeUnit.MILLISECONDS));

        Thread.sleep(1000);

        Node highest = nodes.stream().max(Comparator.comparing(Node::getPriorityId)).get();
        assertTrue(highest.isLeader());
        nodes.stream()
                .filter(node -> node.getPriorityId() != highest.getPriorityId())
                .forEach(node -> assertFalse(node.isLeader()));

        nodes.forEach(node -> ((ElectionCoordinator)node).shutdown());
    }

    @Test
    public void electLeaderWhileHighestPriorityMediumSize() throws InterruptedException {
        List<Node> nodes = createNodes(50, 2,0,0);
        nodes.forEach(node -> ((ElectionCoordinator)node).start(100, 100, TimeUnit.MILLISECONDS));

        Thread.sleep(3000);

        Node highest = nodes.stream().max(Comparator.comparing(Node::getPriorityId)).get();
        assertTrue(highest.isLeader());
        nodes.stream()
                .filter(node -> node.getPriorityId() != highest.getPriorityId())
                .forEach(node -> assertFalse(node.isLeader()));

        nodes.forEach(node -> ((ElectionCoordinator)node).shutdown());
    }


    @Test
    public void electLeaderWithFaultyNode() throws InterruptedException {

        List<Node> nodes = new ArrayList<>();
        for(int i=0; i<3; i++) {
            nodes.add(new ElectionCoordinator((long)random.nextInt(500), 3));
        }

        for(int i=0; i<3; i++) {
            ElectionCoordinator electionCoordinator = new ElectionCoordinator(500l+ (long)random.nextInt(500), 2);
            electionCoordinator.setFaultyState();
            nodes.add(electionCoordinator);
        }

        nodes.forEach(node -> ((ElectionCoordinator)node).addAll(nodes));
        nodes.forEach(node -> ((ElectionCoordinator)node).start(100, 100, TimeUnit.MILLISECONDS));

        Thread.sleep(1000);

        Node highest = nodes.stream().filter(node -> node.getPriorityId() < 500).max(Comparator.comparing(Node::getPriorityId)).get();
        assertTrue(highest.isLeader());
        nodes.stream()
                .filter(node -> node.getPriorityId() != highest.getPriorityId())
                .forEach(node -> assertFalse(node.isLeader()));

        nodes.forEach(node -> ((ElectionCoordinator)node).shutdown());
    }

    @Test
    public void electNewLeaderAfterCurrentOneFails() throws InterruptedException {
        List<Node> nodes = createNodes(5,5,0,0);
        nodes.forEach(node -> ((ElectionCoordinator)node).start(100, 100, TimeUnit.MILLISECONDS));

        Thread.sleep(1000);

        Node highest = nodes.stream().max(Comparator.comparing(Node::getPriorityId)).get();
        assertTrue(highest.isLeader());
        nodes.stream()
                .filter(node -> node.getPriorityId() != highest.getPriorityId())
                .forEach(node -> assertFalse(node.isLeader()));

        ((ElectionCoordinator)highest).setFaultyState();
        Thread.sleep(1000);

        Node secondHighest = nodes.stream()
                .filter(node -> node.getPriorityId() != highest.getPriorityId())
                .max(Comparator.comparing(Node::getPriorityId)).get();
        assertTrue(secondHighest.isLeader());
        nodes.stream()
                .filter(node -> node.getPriorityId() != secondHighest.getPriorityId())
                .forEach(node -> assertFalse(node.isLeader()));

        nodes.forEach(node -> ((ElectionCoordinator)node).shutdown());
    }

    @Test
    public void electLeaderWithShortDelay() throws InterruptedException {
        List<Node> nodes = createNodes(5, 5,20,0);
        nodes.forEach(node -> ((ElectionCoordinator)node).start(100, 100, TimeUnit.MILLISECONDS));
        Thread.sleep(1000);

        Node highest = nodes.stream().max(Comparator.comparing(Node::getPriorityId)).get();
        assertTrue(highest.isLeader());
        nodes.stream()
                .filter(node -> node.getPriorityId() != highest.getPriorityId())
                .forEach(node -> assertFalse(node.isLeader()));
        nodes.forEach(node -> ((ElectionCoordinator)node).shutdown());
    }

    @Test
    public void electLeaderWithLongDelay() throws InterruptedException {
        List<Node> nodes = createNodes(5, 5,200,0);
        nodes.forEach(node -> ((ElectionCoordinator)node).start(100, 100, TimeUnit.MILLISECONDS));
        Thread.sleep(2000);

        Node highest = nodes.stream().max(Comparator.comparing(Node::getPriorityId)).get();
        assertTrue(highest.isLeader());
        nodes.stream()
                .filter(node -> node.getPriorityId() != highest.getPriorityId())
                .forEach(node -> assertFalse(node.isLeader()));
        nodes.forEach(node -> ((ElectionCoordinator)node).shutdown());
    }

    @Test
    public void electLeaderWithDrops() throws InterruptedException {
        List<Node> nodes = createNodes(5, 5,10,10);
        nodes.forEach(node -> ((ElectionCoordinator)node).start(100, 100, TimeUnit.MILLISECONDS));
        Thread.sleep(3000);

        long count = nodes.stream().filter(node -> node.isLeader()).count();
        Assert.assertEquals(1, count);
        nodes.forEach(node -> ((ElectionCoordinator)node).shutdown());
    }


}
