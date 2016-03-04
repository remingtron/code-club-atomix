package com.codeclub;

import io.atomix.AtomixReplica;
import io.atomix.catalyst.transport.Address;
import io.atomix.coordination.DistributedGroup;
import io.atomix.coordination.LocalGroupMember;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

@Controller
public class AtomixController {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${atomix.port}")
    int atomixPort;

    private AtomixReplica replica;

    private Map<String, LocalGroupMember> members = new HashMap<>();

    @PostConstruct
    public void init() {
        List<Address> members = asList(9001, 9002, 9003).stream().map(p -> new Address("localhost", p)).collect(Collectors.toList());
        replica = AtomixReplica.builder(new Address("localhost", atomixPort), members)
                .withStorage(new Storage(StorageLevel.MEMORY))
                .build();

        replica.open().join();

        logger.info("It's happening! The replica is happening!");
    }

    @RequestMapping("/join-group/{group}")
    @ResponseStatus(code = HttpStatus.OK)
    public void joinGroup(@PathVariable String group) {
        CompletableFuture<DistributedGroup> groupCompletableFuture = replica.getGroup(group);
        DistributedGroup distributedGroup = groupCompletableFuture.join();
        CompletableFuture<LocalGroupMember> memberCompletableFuture = distributedGroup.join();
        LocalGroupMember member = memberCompletableFuture.join();

        logger.info("Thank you for joining '" + group + "'. We hope you enjoy your stay.");

        member.onElection(t -> {
            logger.info("Bow down to me, people of '" + group + "'");
        });

        members.put(group, member);
    }

    @RequestMapping("/leave-group/{group}")
    @ResponseStatus(code = HttpStatus.OK)
    public void leaveGroup(@PathVariable String group) {
        logger.info("Sorry, I gots to go. Peace out, '" + group + "'");
        members.get(group).leave();
    }

    @RequestMapping("/resign/{group}")
    @ResponseStatus(code = HttpStatus.OK)
    public void resign(@PathVariable String group) {
        logger.info("I am no longer fit to lead, '" + group + "'");
        members.get(group).resign();
    }
}
