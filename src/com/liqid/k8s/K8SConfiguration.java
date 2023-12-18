/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s;

import com.bearsnake.k8sclient.K8SClient;
import com.bearsnake.k8sclient.K8SHTTPError;
import com.bearsnake.k8sclient.K8SJSONError;
import com.bearsnake.k8sclient.K8SRequestError;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Represents the information we're interested in, derived from a configuration file.
 */
// TODO probably don't need this anymore
//public class K8SConfiguration {
//
//    private final Logger _logger;
//    private final Collection<ConfigurationNode> _nodes = new LinkedList<>();
//
//    public K8SConfiguration(
//        final Logger logger
//    ) {
//        _logger = logger;
//    }
//
//    public Collection<ConfigurationNode> getNodes() { return new LinkedList<>(_nodes); }
//
//    public void load(
//        final K8SClient k8sClient
//    ) throws K8SRequestError, K8SJSONError, K8SHTTPError {
//        _nodes.clear();
//        var k8sNodes = k8sClient.getNodes();
//        for (var nodeEntity : k8sNodes) {
//            var metadata = nodeEntity.metadata;
//            var nodeName = metadata.name;
//            var computeName = metadata.annotations.get("liqid.compute-name");
//            try {
//                if (computeName != null) {
//                    boolean isControl = false;
//                    for (var label : metadata.labels.keySet()) {
//                        if (label.contains("kubernetes.io")
//                            && (label.contains("control-plane") || label.contains("master"))) {
//                            isControl = true;
//                            break;
//                        }
//                    }
//
//                    boolean isWorker = !isControl;
//
//                    ConfigurationNode node = new ConfigurationNode(nodeName, isControl, isWorker, computeName);
//
//                    var fpgaStr = metadata.annotations.get("liqid.fpga-count");
//                    if (fpgaStr != null) {
//                        node._specifiedFPGACount = Integer.parseInt(fpgaStr);
//                    }
//
//                    var gpuStr = metadata.annotations.get("liqid.gpu-count");
//                    if (gpuStr != null) {
//                        node._specifiedGPUCount = Integer.parseInt(gpuStr);
//                    }
//
//                    var linkStr = metadata.annotations.get("liqid.link-count");
//                    if (linkStr != null) {
//                        node._specifiedLinkCount = Integer.parseInt(linkStr);
//                    }
//
//                    var memoryStr = metadata.annotations.get("liqid.memory-count");
//                    if (memoryStr != null) {
//                        node._specifiedMemoryCount = Integer.parseInt(memoryStr);
//                    }
//
//                    var targetStr = metadata.annotations.get("liqid.target-count");
//                    if (targetStr != null) {
//                        node._specifiedSSDCount = Integer.parseInt(targetStr);
//                    }
//
//                    _nodes.add(node);
//                }
//            } catch (NumberFormatException nfe) {
//                var msg = String.format("Node %s has an invalid liqid resource count in the annotations", nodeName);
//                _logger.error(msg);
//            }
//        }
//    }
//
//    @Override
//    public String toString() {
//        var sb = new StringBuilder();
//        sb.append("[");
//        var first = true;
//        for (var n : _nodes) {
//            if (!first) sb.append(",");
//            sb.append(n.toString());
//            first = false;
//        }
//        sb.append("]");
//        return sb.toString();
//    }
//
//    public void display(
//        final String indent
//    ) {
//        for (var entry : _nodes) {
//            System.out.printf("%s%s\n", indent, entry.toString());
//        }
//    }
//}
