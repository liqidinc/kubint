/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.annotate;

import com.bearsnake.k8sclient.K8SException;
import com.bearsnake.k8sclient.K8SHTTPError;
import com.bearsnake.k8sclient.Node;
import com.bearsnake.k8sclient.Pod;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.commands.Command;
import com.liqid.k8s.plan.Plan;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.TreeMap;

import static com.liqid.k8s.Constants.K8S_ANNOTATION_PREFIX;
import static com.liqid.k8s.Constants.K8S_CONFIG_NAME;
import static com.liqid.k8s.Constants.K8S_CONFIG_NAMESPACE;

//class NodesCommand extends Command {
//
//    NodesCommand(
//        final Logger logger,
//        final String proxyURL,
//        final Boolean force,
//        final Integer timeoutInSeconds
//    ) {
//        super(logger, proxyURL, force, timeoutInSeconds);
//    }
//
//    @Override
//    public Plan process(
//    ) throws K8SException {
//        var fn = this.getClass().getName() + ":process";
//        _logger.trace("Entering %s", fn);
//
//        initK8sClient();
//
//        try {
//            System.out.println("Liqid Cluster linkage settings:");
//            var configMap = _k8sClient.getConfigMap(K8S_CONFIG_NAMESPACE, K8S_CONFIG_NAME);
//            for (var entry : configMap.data.entrySet()) {
//                System.out.printf("  %s: %s\n", entry.getKey(), entry.getValue());
//            }
//        } catch (K8SHTTPError ex) {
//            if (ex.getResponseCode() == 404) {
//                System.out.println("  <None established>");
//            } else {
//                throw ex;
//            }
//        }
//
//        var nodes = _k8sClient.getNodes();
//        var pods = _k8sClient.getPods();
//
//        var nodeMap = new TreeMap<String, Node>(); // nodes by node name
//        var podMap = new HashMap<Node, LinkedList<Pod>>(); // list of pods by owning node
//        for (var node : nodes) {
//            nodeMap.put(node.getName(), node);
//            podMap.put(node, new LinkedList<>());
//        }
//        for (var pod : pods) {
//            var node = nodeMap.get(pod.spec.nodeName);
//            podMap.get(node).add(pod);
//        }
//
//        for (var node : nodes) {
//            var nodeName = node.metadata.name;
//            System.out.println("Node " + nodeName);
//            System.out.println("  Pods:");
//            for (var pod : podMap.get(node)) {
//                System.out.printf("    %s/%s  %s\n", pod.metadata.namespace, pod.getName(), pod.status.phase);
//            }
//
//            System.out.println("  Liqid Annotations:");
//            var annotations = node.metadata.annotations;
//            var hasEntry = false;
//            for (var entry : annotations.entrySet()) {
//                if (entry.getKey().startsWith(K8S_ANNOTATION_PREFIX)) {
//                    System.out.printf("    %s: %s\n", entry.getKey(), entry.getValue());
//                    hasEntry = true;
//                }
//            }
//
//            if (!hasEntry) {
//                System.out.println("  <Node has no liqid-specific annotations>");
//            }
//        }
//
//        _logger.trace("Exiting %s with null", fn);
//        return null;
//    }
//}
