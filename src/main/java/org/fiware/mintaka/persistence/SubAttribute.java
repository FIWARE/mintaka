package org.fiware.mintaka.persistence;

import lombok.Data;
import org.geojson.*;
import org.hibernate.annotations.Formula;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.time.Instant;

@Data
@Entity
@Table(name = "subattributes")
public class SubAttribute {

    @NotNull
    @Id
    @Column(name = "instanceid")
    private String instanceId;

    @NotNull
    private String id;

    @NotNull
    @Column(name = "entityid")
    private String entityId;

    @NotNull
    @Column(name = "attributeid")
    private String attributeId;

    @Column(name = "observedat")
    private Instant observedAt;

    @Column(name = "unitcode")
    private String unitCode;

    @Column(name = "datasetid")
    private String datasetId;

    @Column(name = "valuetype")
    private ValueType valueType;

    @Column(name = "boolean")
    private Boolean aBoolean;

    @Column(name = "number")
    private Double number;

    private Instant datetime;

    @Column(columnDefinition = "jsonb")
    private String compound;

    @Formula(value = "ST_AsGeoJSON(geopoint)")
    @Convert(converter = JacksonGeoJsonConverter.class)
    private Point geoPoint;

    @Formula(value = "ST_AsGeoJSON(geopolygon)")
    @Convert(converter = JacksonGeoJsonConverter.class)
    private Polygon geoPolygon;

    @Formula(value = "ST_AsGeoJSON(geomultipolygon)")
    @Convert(converter = JacksonGeoJsonConverter.class)
    private MultiPolygon geoMultiPolygon;

    @Formula(value = "ST_AsGeoJSON(geolinestring)")
    @Convert(converter = JacksonGeoJsonConverter.class)
    private LineString geoLineString;

    @Formula(value = "ST_AsGeoJSON(geomultilinestring)")
    @Convert(converter = JacksonGeoJsonConverter.class)
    private MultiLineString geoMultiLineString;

    @NotNull
    private Instant ts;
}
