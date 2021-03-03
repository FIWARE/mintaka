package org.fiware.mintaka.domain;

import org.fiware.mintaka.domain.query.temporal.TimeRelation;
import org.fiware.ngsi.model.TimerelVO;
import org.mapstruct.Mapper;

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

}
