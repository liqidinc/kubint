/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.plan.actions;

import com.bearsnake.k8sclient.K8SException;
import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.exceptions.ProcessingException;
import com.liqid.k8s.plan.ExecutionContext;

import java.util.HashMap;
import java.util.Map;

public class AnnotateNodeAction extends Action {

    private String _nodeName;
    private Map<String, String> _annotations = new HashMap<>();

    public AnnotateNodeAction() {
        super(ActionType.ANNOTATE_NODE);
    }

    public AnnotateNodeAction addAnnotation(
        final String keySuffix,
        final String value
    ) {
        _annotations.put(keySuffix, value);
        return this;
    }

    public Map<String, String> getAnnotations() { return _annotations; }
    public String getNodeName() { return _nodeName; }
    public AnnotateNodeAction setNodeName(final String value) {_nodeName = value; return this; }

    @Override
    public void checkParameters() throws InternalErrorException {
        checkForNull("NodeName", _nodeName);
    }

    @Override
    public void perform(
        final ExecutionContext context
    ) throws K8SException, ProcessingException {
        var fn = this.getClass().getName() + ":perform";
        context.getLogger().trace("Entering %s", fn);

        var realAnnotations = new HashMap<String, String>();
        for (var entry : _annotations.entrySet()) {
            realAnnotations.put(createAnnotationKeyFor(entry.getKey()), entry.getValue());
        }

        System.out.printf("Writing annotations to %s:\n", _nodeName);
        for (var entry : realAnnotations.entrySet()) {
            if (entry.getValue() != null) {
                System.out.printf("  %s=%s\n", entry.getKey(), entry.getValue());
            } else {
                System.out.printf("  %s-\n", entry.getKey());
            }
        }

        context.getK8SClient().updateAnnotationsForNode(_nodeName, realAnnotations);
        context.getLogger().trace("%s returning", fn);
    }

    @Override
    public String toString() {
        var sb = new StringBuilder();
        sb.append("Update annotations for Kubernetes node ").append(_nodeName);
        for (var entry : _annotations.entrySet()) {
            sb.append("\n      ").append(entry.getKey()).append(":");
            if (entry.getValue() == null) {
                sb.append("<to be removed>");
            } else {
                sb.append(entry.getValue());
            }
        }
        return sb.toString();
    }
}
