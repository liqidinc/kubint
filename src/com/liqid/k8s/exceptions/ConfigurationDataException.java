/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.exceptions;

/**
 * Indicates that we've found nonsense data in some portion of the K8s or Liqid Configuration,
 * and we cannot continue.
 */
public class ConfigurationDataException extends ScriptException {

    public ConfigurationDataException(final String message) {
        super(message);
    }
}
