/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.sdk.Machine;

public class MachineProfile extends Profile {

    private final Machine _machine;

    public MachineProfile(
        final Machine machine
    ) {
        _machine = machine;
    }

    public Machine getMachine() { return _machine; }
}
