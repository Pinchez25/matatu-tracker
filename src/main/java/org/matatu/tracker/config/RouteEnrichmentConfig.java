package org.matatu.tracker.config;

import java.util.Map;

import org.matatu.tracker.model.SaccoInfo;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RouteEnrichmentConfig {

    @Bean
    public Map<String, SaccoInfo> saccoLookup() {
        return Map.of(
                "route_33", new SaccoInfo("sacco_01", "Citi Hoppa", "Kikuyu Town"),
                "route_23", new SaccoInfo("sacco_02", "KBS", "Westlands"),
                "route_58", new SaccoInfo("sacco_03", "Double M", "Kawangware"),
                "route_111", new SaccoInfo("sacco_04", "Metro Trans", "Rongai"),
                "route_46", new SaccoInfo("sacco_05", "Forward Travellers", "Eastleigh"));
    }
}
