package org.fiware.mintaka.persistence;

/**
 * OpMode enum as used by orion-ld
 * {@see https://github.com/FIWARE/context.Orion-LD}
 */
public enum ValueType {
    String,
    Number,
    Boolean,
    Relationship,
    Compound,
    DateTime,
    GeoPoint,
    GeoPolygon,
    GeoMultiPolygon,
    GeoLineString,
    GeoMultiLineString,
    LanguageMap
}
