package org.fiware.mintaka.persistence;

/**
 * OpMode enum as used by <a href="https://github.com/FIWARE/context.Orion-LD">Orion-LD</a>
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
