package org.beroot.beans;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Ban {
    private String id;
    private String streetNumber;
    private String street1;
    private String zipCode;
    private String city;
    private String source;
    private GpsPoint location;
}
