package org.fiware.mintaka.domain;

import org.fiware.ngsi.model.GeometryEnumVO;
import org.fiware.ngsi.model.TimerelVO;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;
import org.mapstruct.ValueMapping;

/**
 * Maps objects between the (NGSI)api-domain and the internal domain.
 */
@Mapper(componentModel = "jsr330")
public interface ApiDomainMapper {

	/**
	 * Get an internal timerelation object from the api's timerelVO
	 *
	 * @param timerelVO - the api object
	 * @return the internal representation
	 */
	TimeRelation timeRelVoToTimeRelation(TimerelVO timerelVO);

	/**
	 * Get an internal geometry enum from the apis geometryEnumVO
	 *
	 * @param geometryEnumVO - the api object
	 * @return the internal representation
	 */
	@Mapping(source = "MULTIPOINT", target = "LINESTRING")
	Geometry geometryEnumVoToGeometry(GeometryEnumVO geometryEnumVO);

}
