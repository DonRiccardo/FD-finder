package com.example.server;

import com.netflix.discovery.shared.Applications;
import com.netflix.eureka.EurekaServerContextHolder;
import com.netflix.eureka.registry.PeerAwareInstanceRegistry;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;


import java.util.ArrayList;
import java.util.List;

@CrossOrigin("http://localhost:5173")
@RestController
public class ServerController {


    @GetMapping("/algorithms")
    public List<String> algorithms(){
        PeerAwareInstanceRegistry registry = EurekaServerContextHolder.getInstance().getServerContext().getRegistry();
        Applications applications = registry.getApplications();

        List<String> algorithms = new ArrayList<>();

        applications.getRegisteredApplications().forEach((registeredApplication) -> {
            registeredApplication.getInstances().forEach((instance) -> {
                if (instance.getAppName().toLowerCase().startsWith("algservice-")) {
                    algorithms.add(instance.getAppName().toLowerCase().substring("algservice-".length()));
                }

            });
        });

        return algorithms;
    }
}
