/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

// describes an ellocation - for a machine, it is a list of device identifiers which are,
// or which are to be, attached to that machine.
public class Allocation {

    // the machine to which this applies
    private final String _machineName;

    // device identifiers
    private final Set<Integer> _deviceIdentifiers = new HashSet<>();

    public Allocation(
        final String machineName,
        final Collection<Integer> deviceIdentifiers
    ) {
        _machineName = machineName;
        _deviceIdentifiers.addAll(deviceIdentifiers);
    }

    public Allocation(
        final String machineName
    ) {
        _machineName = machineName;
    }

    public Allocation appendDeviceIdentifier(
        final Integer identifier
    ) {
        _deviceIdentifiers.add(identifier);
        return this;
    }

    public Allocation appendDeviceIdentifiers(
        final Collection<Integer> identifiers
    ) {
        _deviceIdentifiers.addAll(identifiers);
        return this;
    }

    public Collection<Integer> getDeviceIdentifiers() {
        return new HashSet<>(_deviceIdentifiers);
    }

    public String getMachineName() {
        return _machineName;
    }

    @Override
    public String toString() {
        return String.format("%s <- %s", _machineName, _deviceIdentifiers);
    }
}
