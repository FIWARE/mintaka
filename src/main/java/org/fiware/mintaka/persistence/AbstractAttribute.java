package org.fiware.mintaka.persistence;

import lombok.Data;
import org.geojson.*;
import org.hibernate.annotations.Formula;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.time.LocalDateTime;

/**
 * Database representation of an attribute as parent for the concrete SubAttributes and Attributes. Schema is defined by
 * <a href="https://github.com/FIWARE/context.Orion-LD">Orion-LD</a>
 */
@Data
@MappedSuperclass
public abstract class AbstractAttribute {

	@NotNull
	@Id
	@Column(name = "instanceid")
	private String instanceId;
	@NotNull
	private String id;

	@NotNull
	@Column(name = "entityid")
	private String entityId;

	@Column(name = "observedat")
	private LocalDateTime observedAt;

	@Column(name = "unitcode")
	private String unitCode;

	@Enumerated(EnumType.STRING)
	@Column(name = "valuetype")
	private ValueType valueType;

	@Column(name = "boolean")
	private Boolean aBoolean;

	@Column(name = "number")
	private Double number;

	private String text;

	private LocalDateTime datetime;

	@Column(columnDefinition = "jsonb")
	@Convert(converter = JacksonJsonBConverter.class)
	private Object compound;

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
	private LocalDateTime ts;
}
