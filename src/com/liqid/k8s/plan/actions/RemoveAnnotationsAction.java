/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.plan.actions;

import com.bearsnake.k8sclient.K8SHTTPError;
import com.bearsnake.k8sclient.K8SJSONError;
import com.bearsnake.k8sclient.K8SRequestError;
import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.plan.ExecutionContext;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import static com.liqid.k8s.Constants.K8S_ANNOTATION_PREFIX;

public class RemoveAnnotationsAction extends Action {

    private Set<String> _nodeNames = new TreeSet<>();

    public RemoveAnnotationsAction() {
        super(ActionType.REMOVE_ANNOTATIONS);
    }

    public RemoveAnnotationsAction addNodeName(final String value) { _nodeNames.add(value); return this; }
    public RemoveAnnotationsAction setNodeNames(final Collection<String> list) {_nodeNames = new TreeSet<>(list); return this; }

    @Override
    public void checkParameters() throws InternalErrorException {
        checkForNull("NodeNames", _nodeNames);
    }

    @Override
    public void perform(
        final ExecutionContext context
    ) throws InternalErrorException, K8SHTTPError, K8SJSONError, K8SRequestError {
        var fn = this.getClass().getName() + ":perform";
        context.getLogger().trace("Entering %s", fn);

        for (var node : context.getK8SClient().getNodes()) {
            var annotations = node.metadata.annotations;
            var changed = false;
            for (java.util.Map.Entry<String, String> entry : annotations.entrySet()) {
                if (entry.getKey().startsWith(K8S_ANNOTATION_PREFIX)) {
                    annotations.put(entry.getKey(), null);
                    changed = true;
                }
            }
            if (changed) {
                System.out.println("Removing Liqid annotations from node '" + node.getName() + "'...");
                context.getK8SClient().updateAnnotationsForNode(node.getName(), annotations);
            }
        }

        context.getLogger().trace("%s returning", fn);
    }

    @Override
    public String toString() {
        return String.format("Remove all Liqid annotations from Kubernetes node%s %s",
                             (_nodeNames.size() == 1) ? "" : "s",
                             String.join(", ", _nodeNames));
    }
}
