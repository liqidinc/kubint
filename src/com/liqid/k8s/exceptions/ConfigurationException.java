/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.exceptions;

/**
 * Indicates that we've found an inconsistency in, or a missing, configuration element
 * (such as a Liqid cluster not being linked to the Kubernetes cluster)
 * and we cannot continue processing.
 */
public class ConfigurationException extends ScriptException {

    public ConfigurationException(final String message) {
        super(message);
    }
}
