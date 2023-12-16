/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.annotate;

import com.bearsnake.k8sclient.ConfigMapPayload;
import com.bearsnake.k8sclient.K8SHTTPError;
import com.bearsnake.k8sclient.K8SJSONError;
import com.bearsnake.k8sclient.K8SRequestError;
import com.bearsnake.k8sclient.NamespacedMetadata;
import com.bearsnake.k8sclient.SecretPayload;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.Command;
import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.sdk.LiqidException;

import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;

import static com.liqid.k8s.Constants.K8S_CONFIG_MAP_GROUP_NAME_KEY;
import static com.liqid.k8s.Constants.K8S_CONFIG_MAP_IP_ADDRESS_KEY;
import static com.liqid.k8s.Constants.K8S_CONFIG_NAME;
import static com.liqid.k8s.Constants.K8S_CONFIG_NAMESPACE;
import static com.liqid.k8s.Constants.K8S_SECRET_CREDENTIALS_KEY;
import static com.liqid.k8s.Constants.K8S_SECRET_NAME;
import static com.liqid.k8s.Constants.K8S_SECRET_NAMESPACE;
import static com.liqid.k8s.annotate.CommandType.LINK;

class LinkCommand extends Command {

    LinkCommand(
        final Logger logger,
        final String proxyURL,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, proxyURL, force, timeoutInSeconds);
    }

    LinkCommand setLiqidAddress(final String value) { _liqidAddress = value; return this; }
    LinkCommand setLiqidGroupName(final String value) { _liqidGroupName = value; return this; }
    LinkCommand setLiqidPassword(final String value) { _liqidPassword = value; return this; }
    LinkCommand setLiqidUsername(final String value) { _liqidUsername = value; return this; }

    @Override
    public boolean process() throws InternalErrorException, K8SHTTPError, K8SJSONError, K8SRequestError {
        var fn = LINK.getToken() + ":process";
        _logger.trace("Entering %s", fn);

        if (_liqidAddress == null) {
            throw new InternalErrorException("Liqid Address is null");
        }

        if (_liqidGroupName == null) {
            throw new InternalErrorException("Liqid Group name is null");
        }

        if (!initK8sClient()) {
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        // If there is already a configMap with this cluster name...
        //      If force is set, write warning, delete the existing info, and continue
        //      Otherwise tell user it already exists, and stop
        try {
            var oldConfigMap = _k8sClient.getConfigMap(K8S_CONFIG_NAMESPACE, K8S_CONFIG_NAME);
            if (oldConfigMap != null) {
                if (!_force) {
                    System.err.println("ERROR:A link already exists between the Kubernetes Cluster and the Liqid Cluster.");
                    for (var e : oldConfigMap.data.entrySet()) {
                        System.err.printf("     : %s = %s\n", e.getKey(), e.getValue());
                    }
                    _logger.trace("Exiting %s false", fn);
                    return false;
                }

                System.err.println("WARNING:A link already exists between the Kubernetes Cluster and the Liqid Cluster.");
                System.err.println("       :The existing link will be deleted, and a new one created.");
                _k8sClient.deleteConfigMap(K8S_CONFIG_NAMESPACE, K8S_CONFIG_NAME);
            }
        } catch (K8SHTTPError ex) {
            //  We *should* get here with a 404. Anything other than a 404 is a Bad Thing.
            if (ex.getResponseCode() != 404) {
                throw ex;
            }
        }

        // Is there a configMap? If so, get rid of it.
        try {
            _k8sClient.deleteSecret(K8S_SECRET_NAMESPACE, K8S_SECRET_NAME);
        } catch (K8SHTTPError ex) {
            //  A 404 is okay - anything else is not
            if (ex.getResponseCode() != 404) {
                throw ex;
            }
        }

        // Now go verify that the link is correct (i.e., that we can contact the Liqid Director)
        try {
            if (!initLiqidClient()) {
                if (_force) {
                    System.err.println("WARNING:Cannot connect to the Liqid Cluster, but proceeding anyway.");
                } else {
                    System.err.println("ERROR:Cannot connect to the Liqid Cluster - stopping.");
                    _logger.trace("Exiting %s false", fn);
                    return false;
                }
            } else {
                // Logged in - see if the indicated group exists.
                var groups = _liqidClient.getGroups();
                boolean found = false;
                for (var group : groups) {
                    if (group.getGroupName().equals(_liqidGroupName)) {
                        found = true;
                        break;
                    }
                }

                if (!found) {
                    if (_force) {
                        System.err.println("WARNING:Group " + _liqidGroupName + " does not exist on the Liqid Cluster and will be created");
                        _liqidClient.createGroup(_liqidGroupName);
                    } else {
                        System.err.println("ERROR:Group " + _liqidGroupName + " does not exist on the Liqid Cluster");
                        _logger.trace("Exiting %s false", fn);
                        return false;
                    }
                }
            }
        } catch (LiqidException ex) {
            _logger.catching(ex);
            if (_force) {
                System.err.println("WARNING:Cannot connect to Liqid Cluster - proceeding anyway due to force being set");
            } else {
                System.err.println("ERROR:Cannot connect to Liqid Cluster - stopping");
                _logger.trace("Exiting %s false", fn);
                return false;
            }
        }

        // Write the configMap
        var cfgMapData = new HashMap<String, String>();
        cfgMapData.put(K8S_CONFIG_MAP_IP_ADDRESS_KEY, _liqidAddress);
        cfgMapData.put(K8S_CONFIG_MAP_GROUP_NAME_KEY, _liqidGroupName);
        var cmMetadata = new NamespacedMetadata().setNamespace(K8S_CONFIG_NAMESPACE).setName(K8S_CONFIG_NAME);
        var newCfgMap = new ConfigMapPayload().setMetadata(cmMetadata).setData(cfgMapData);
        _k8sClient.createConfigMap(newCfgMap);

        // If there are credentials, write a secret
        if (_liqidUsername != null) {
            var str = _liqidUsername;
            if (_liqidPassword != null) {
                str += ":" + _liqidPassword;
            }

            var secretData = Collections.singletonMap(K8S_SECRET_CREDENTIALS_KEY, Base64.getEncoder().encodeToString(str.getBytes()));
            var secretMetadata = new NamespacedMetadata().setNamespace(K8S_SECRET_NAMESPACE).setName(K8S_SECRET_NAME);
            var newSecret = new SecretPayload().setMetadata(secretMetadata).setData(secretData);
            _k8sClient.createSecret(newSecret);
        }

        // All done
        logoutFromLiqidCluster();
        _logger.trace("Exiting %s true", fn);
        return true;
    }
}
