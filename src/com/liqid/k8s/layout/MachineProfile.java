/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.sdk.Machine;

/**
 * A Profile object tied to a particular machine
 */
public class MachineProfile extends Profile {

    private final String _machineName;

    public MachineProfile(
        final String machineName
    ) {
        _machineName = machineName;
    }

    public String getMachineName() { return _machineName; }
}
