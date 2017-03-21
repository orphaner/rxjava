package org.beroot.beans;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class GpsPoint {
    private Double lat;
    private Double lon;
}
