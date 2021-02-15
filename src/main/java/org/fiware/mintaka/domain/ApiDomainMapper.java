package org.fiware.mintaka.domain;

import org.fiware.ngsi.model.TimerelVO;
import org.mapstruct.Mapper;

@Mapper(componentModel = "jsr330")
public interface ApiDomainMapper {

	TimeRelation timeRelVoToTimeRelation(TimerelVO timerelVO);

	TimerelVO timeRelationToTimeRelVo(TimeRelation timeRelation);
}
