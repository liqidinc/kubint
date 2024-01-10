/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.plan.actions;

import com.bearsnake.k8sclient.K8SHTTPError;
import com.bearsnake.k8sclient.K8SRequestError;
import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.plan.ExecutionContext;

import static com.liqid.k8s.Constants.K8S_CONFIG_NAME;
import static com.liqid.k8s.Constants.K8S_CONFIG_NAMESPACE;
import static com.liqid.k8s.Constants.K8S_SECRET_NAME;
import static com.liqid.k8s.Constants.K8S_SECRET_NAMESPACE;

public class RemoveLinkage extends Action {

    public RemoveLinkage() {
        super(ActionType.REMOVE_LINKAGE);
    }

    @Override
    public void checkParameters(){}

    @Override
    public void perform(
        final ExecutionContext context
    ) throws InternalErrorException, K8SHTTPError, K8SRequestError {
        var fn = this.getClass().getName() + ":perform";
        context.getLogger().trace("Entering %s", fn);

        try {
            System.out.println("Deleting ConfigMap if it exists...");
            context.getK8SClient().deleteConfigMap(K8S_CONFIG_NAMESPACE, K8S_CONFIG_NAME);
        } catch (K8SHTTPError ex) {
            if (ex.getResponseCode() != 404) {
                throw ex;
            }
        }

        try {
            System.out.println("Deleting Secret if it exists...");
            context.getK8SClient().deleteSecret(K8S_SECRET_NAMESPACE, K8S_SECRET_NAME);
        } catch (K8SHTTPError ex) {
            if (ex.getResponseCode() != 404) {
                throw ex;
            }
        }

        context.getLogger().trace("%s returning", fn);
    }

    @Override
    public String toString() {
        return "Remove linkage between Kubernetes Cluster and Liqid Cluster";
    }
}
