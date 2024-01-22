/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.plan.actions;

import com.bearsnake.k8sclient.ConfigMapPayload;
import com.bearsnake.k8sclient.K8SHTTPError;
import com.bearsnake.k8sclient.K8SRequestError;
import com.bearsnake.k8sclient.NamespacedMetadata;
import com.bearsnake.k8sclient.SecretPayload;
import com.liqid.k8s.CredentialMangler;
import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.plan.ExecutionContext;

import java.util.Collections;
import java.util.HashMap;

import static com.liqid.k8s.Constants.K8S_CONFIG_MAP_GROUP_NAME_KEY;
import static com.liqid.k8s.Constants.K8S_CONFIG_MAP_IP_ADDRESS_KEY;
import static com.liqid.k8s.Constants.K8S_CONFIG_NAME;
import static com.liqid.k8s.Constants.K8S_CONFIG_NAMESPACE;
import static com.liqid.k8s.Constants.K8S_SECRET_CREDENTIALS_KEY;
import static com.liqid.k8s.Constants.K8S_SECRET_NAME;
import static com.liqid.k8s.Constants.K8S_SECRET_NAMESPACE;

public class CreateLinkageAction extends Action {

    private String _liqidAddress;
    private String _liqidGroupName;
    private String _liqidPassword;
    private String _liqidUsername;

    public CreateLinkageAction() {
        super(ActionType.CREATE_LINKAGE);
    }

    public CreateLinkageAction setLiqidAddress(final String value) {_liqidAddress = value; return this; }
    public CreateLinkageAction setLiqidGroupName(final String value) {_liqidGroupName = value; return this; }
    public CreateLinkageAction setLiqidPassword(final String value) {_liqidPassword = value; return this; }
    public CreateLinkageAction setLiqidUsername(final String value) {_liqidUsername = value; return this; }

    @Override
    public void checkParameters() throws InternalErrorException {
        checkForNull("LiqidAddress", _liqidAddress);
        checkForNull("LiqidGroupName", _liqidGroupName);
        if (_liqidPassword != null) {
            checkForNull("LiqidUsername", _liqidUsername);
        }
    }

    @Override
    public void perform(
        final ExecutionContext context
    ) throws K8SHTTPError, K8SRequestError {
        var fn = this.getClass().getName() + ":perform";
        context.getLogger().trace("Entering %s", fn);

        // Write the configMap
        var cfgMapData = new HashMap<String, String>();
        cfgMapData.put(K8S_CONFIG_MAP_IP_ADDRESS_KEY, _liqidAddress);
        cfgMapData.put(K8S_CONFIG_MAP_GROUP_NAME_KEY, _liqidGroupName);
        var cmMetadata = new NamespacedMetadata().setNamespace(K8S_CONFIG_NAMESPACE).setName(K8S_CONFIG_NAME);
        var newCfgMap = new ConfigMapPayload().setMetadata(cmMetadata).setData(cfgMapData);
        context.getK8SClient().createConfigMap(newCfgMap);

        // If there are credentials, write a secret
        if (_liqidUsername != null) {
            var mangled = new CredentialMangler(_liqidUsername, _liqidPassword).getMangledString();
            var secretData = Collections.singletonMap(K8S_SECRET_CREDENTIALS_KEY, mangled);
            var secretMetadata = new NamespacedMetadata().setNamespace(K8S_SECRET_NAMESPACE).setName(K8S_SECRET_NAME);
            var newSecret = new SecretPayload().setMetadata(secretMetadata).setData(secretData);
            context.getK8SClient().createSecret(newSecret);
        }

        context.getLogger().trace("%s returning", fn);
    }

    @Override
    public String toString() {
        return "Create Linkage between Kubernetes Cluster and Liqid Cluster";
    }
}
