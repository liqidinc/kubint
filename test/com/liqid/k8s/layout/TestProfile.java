/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestProfile {

    @Test
    public void injectCount_SimpleGeneric() {
        Integer fpgaCount = 0;
        var fpgaModel = new ResourceModel(LiqidGeneralType.FPGA);
        Integer gpuCount = 15;
        var gpuModel = new ResourceModel(LiqidGeneralType.GPU);
        Integer linkCount = -27;
        var linkModel = new ResourceModel(LiqidGeneralType.LINK);

        var p = new Profile();
        p.injectCount(LiqidGeneralType.FPGA, fpgaCount);
        p.injectCount(LiqidGeneralType.GPU, gpuCount);
        p.injectCount(linkModel, linkCount);

        assertEquals(3, p.getResourceModels().size());
        assertEquals(fpgaCount, p.getCount(fpgaModel));
        assertEquals(gpuCount, p.getCount(gpuModel));
        assertEquals(linkCount, p.getCount(linkModel));
        assertNull(p.getCount(new ResourceModel(LiqidGeneralType.CPU)));
        assertNull(p.getCount(new ResourceModel(LiqidGeneralType.FPGA, "LMI Logic", "LMI1200")));
    }

    @Test
    public void injectCount_SimpleModel() {
        Integer[] counts = {0, 15, -27, 3, 100392, 42};
        ResourceModel[] models = {
            new ResourceModel(LiqidGeneralType.FPGA, "LMI Logic", "LMI1200"),
            new ResourceModel(LiqidGeneralType.FPGA, "LMI Logic", "LMI1300"),
            new ResourceModel(LiqidGeneralType.FPGA, "NWidia Corpse-aration", "LMI1200"),
            new ResourceModel(LiqidGeneralType.GPU, "AMX", "RTX12"),
            new ResourceModel(LiqidGeneralType.FPGA),
            new ResourceModel(LiqidGeneralType.GPU),
            };

        var p = new Profile();
        for (var x = 0; x < counts.length; x++) {
            p.injectCount(models[x], counts[x]);
        }

        assertEquals(counts.length, p.getResourceModels().size());
        for (var x = 0; x < counts.length; x++) {
            assertEquals(counts[x], p.getCount(models[x]));
        }
    }

    @Test
    public void injectCount_SimpleSpecific() {
        Integer[] counts = {0, 15, -27, 3, 100392, 42};
        ResourceModel[] models = {
            new ResourceModel(LiqidGeneralType.FPGA, "LMI Logic", "LMI1200"),
            new ResourceModel(LiqidGeneralType.FPGA, "LMI Logic", "LMI1300"),
            new ResourceModel(LiqidGeneralType.FPGA, "NWidia Corpse-aration", "LMI1200"),
            new ResourceModel(LiqidGeneralType.GPU, "AMX", "RTX12"),
            new ResourceModel(LiqidGeneralType.FPGA),
            new ResourceModel(LiqidGeneralType.GPU),
            };

        var p = new Profile();
        for (var x = 0; x < counts.length; x++) {
            p.injectCount(models[x].getGeneralType(),
                          models[x].getVendorName(),
                          models[x].getModelName(),
                          counts[x]);
        }

        assertEquals(counts.length, p.getResourceModels().size());
        for (var x = 0; x < counts.length; x++) {
            assertEquals(counts[x], p.getCount(models[x]));
        }
    }

    @Test
    public void get_Varieties() {
        var p = new Profile();
        p.injectCount(LiqidGeneralType.CPU, 5);
        p.injectCount(LiqidGeneralType.CPU, "Motorola", "M68000", 1);
        p.injectCount(LiqidGeneralType.SSD, "Liqid", "HoneyBadger", 27);

        assertEquals((Integer)5, p.getCount(LiqidGeneralType.CPU));
        assertEquals((Integer)5, p.getCount(LiqidGeneralType.CPU, null, null));
        assertEquals((Integer)5, p.getCount(new ResourceModel(LiqidGeneralType.CPU)));
        assertEquals((Integer)5, p.getCount(new ResourceModel(LiqidGeneralType.CPU, null, null)));

        assertEquals((Integer)1, p.getCount(LiqidGeneralType.CPU, "Motorola", "M68000"));
        assertEquals((Integer)1, p.getCount(new ResourceModel(LiqidGeneralType.CPU, "Motorola", "M68000")));

        assertNull(p.getCount(LiqidGeneralType.SSD));
        assertNull(p.getCount(LiqidGeneralType.SSD, null, null));
        assertNull(p.getCount(new ResourceModel(LiqidGeneralType.SSD)));
        assertNull(p.getCount(new ResourceModel(LiqidGeneralType.SSD, null, null)));

        assertEquals((Integer)27, p.getCount(LiqidGeneralType.SSD, "Liqid", "HoneyBadger"));
        assertEquals((Integer)27, p.getCount(new ResourceModel(LiqidGeneralType.SSD, "Liqid", "HoneyBadger")));
    }

    @Test
    public void getResourceModels() {
        ResourceModel[] models = {
            new ResourceModel(LiqidGeneralType.SSD),
            new ResourceModel(LiqidGeneralType.MEMORY),
            new ResourceModel(LiqidGeneralType.LINK),
            new ResourceModel(LiqidGeneralType.CPU),
            new ResourceModel(LiqidGeneralType.CPU, "Zilog", "Z80"),
        };

        var p = new Profile();
        for (var model : models) {
            p.injectCount(model, 0);
        }

        var resModels = p.getResourceModels();
        assertEquals(models.length, resModels.size());
        for (var model : models) {
            assertTrue(resModels.contains(model));
        }
    }

    @Test
    public void injectCount_Sums() {
        var p = new Profile();
        p.injectCount(LiqidGeneralType.CPU, 0);
        p.injectCount(LiqidGeneralType.CPU, 15);
        p.injectCount(LiqidGeneralType.CPU, -3);
        p.injectCount(LiqidGeneralType.FPGA, -12);

        assertEquals((Integer)12, p.getCount(LiqidGeneralType.CPU));
        assertEquals((Integer)(-12), p.getCount(LiqidGeneralType.FPGA));
    }
}
