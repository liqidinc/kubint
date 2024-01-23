/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

// describes an entry to describe one stage of an allocator to be implemented
public class Allocator {

    // the machine to which this applies
    private final String _machineName;

    // the number of devices to be allocated, from the given list
    private final Integer _count;

    // ordered list of device identifiers, from which we allocate.
    // we always allocate from the front of the list.
    private final LinkedList<Integer> _deviceIdentifiers = new LinkedList<>();

    public Allocator(
        final String machineName,
        final Integer count,
        final List<Integer> deviceIdentifiers // ordered list of identifiers
    ) {
        _machineName = machineName;
        _count = count;
        _deviceIdentifiers.addAll(deviceIdentifiers);
    }

    public String getMachineName() {
        return _machineName;
    }

    public Integer getCount() {
        return _count;
    }

    public LinkedList<Integer> getDeviceIdentifiers() {
        return new LinkedList<>(_deviceIdentifiers);
    }

    @Override
    public String toString() {
        return String.format("%s:%d <- %s", _machineName, _count, _deviceIdentifiers);
    }
}
