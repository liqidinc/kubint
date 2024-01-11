/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.sdk.Machine;

public class MachineLayout {

    private final Machine _machine;
    private final Profile _profile;

    public MachineLayout(
        final Machine machine
    ) {
        _machine = machine;
        _profile = new Profile();
    }

    public Machine getMachine() { return _machine; }
    public Profile getProfile() { return _profile; }

    public void show(
        final String indent
    ) {
        System.out.println(indent + _machine.getMachineName());
        _profile.show(indent + "  ");
    }
}
