package org.fiware.mintaka.persistence;

import lombok.RequiredArgsConstructor;
import org.fiware.mintaka.domain.TimeRelation;
import org.fiware.mintaka.exception.InvalidTimeRelationException;
import org.fiware.ngsi.model.LinearRingDefinitionVO;
import org.w3c.dom.Attr;

import javax.inject.Singleton;
import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.persistence.TypedQuery;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
@RequiredArgsConstructor
public class EntityRepository {

	private final EntityManager entityManager;

	public List<NgsiEntity> findById(String entityId) {
		TypedQuery<NgsiEntity> getNgsiEntitiesQuery = entityManager.createQuery("Select entity from NgsiEntity entity where entity.id=:id", NgsiEntity.class);
		getNgsiEntitiesQuery.setParameter("id", entityId);
		return getNgsiEntitiesQuery.getResultList();
	}

	public List<Attribute> findAttributeByEntityId(String entityId, TimeRelation timeRelation, Instant timeAt, Instant endTime, List<String> attributes, Integer lastN, String timeField) {

		String timeQueryPart = getTimeQueryPart(timeRelation, timeAt, endTime);
		if (attributes == null || attributes.isEmpty()) {
			attributes = findAllAttributesForEntity(entityId, timeQueryPart, timeField);
		}

		return attributes.stream()
				.flatMap(attributeId -> findAttributeInstancesForEntity(entityId, attributeId, timeQueryPart, timeField, lastN).stream())
				.collect(Collectors.toList());
	}

	public List<SubAttribute> findSubAttributeInstancesForAttributeAndEntity(String entityId, String attributeId, TimeRelation timeRelation, Instant timeAt, Instant endTime, Integer lastN, String timeField) {
		String timeQueryPart = getTimeQueryPart(timeRelation, timeAt, endTime);
		TypedQuery<SubAttribute> getSubAttributeInstancesQuery =
				entityManager.createQuery(
						"Select subAttribute " +
								"from SubAttribute subAttribute " +
								"where subAttribute.entityId=:entityId " +
								"and subAttribute.attributeId=:attributeId " +
								"and subAttribute.opMode!='" + OpMode.Delete.name() + "' " +
								"and " + timeQueryPart + " " +
								"order by subAttribute.ts desc", SubAttribute.class);
		getSubAttributeInstancesQuery.setParameter("entityId", entityId);
		getSubAttributeInstancesQuery.setParameter("attributeId", attributeId);
		getSubAttributeInstancesQuery.setParameter("timeField", timeField);
		if (lastN != null && lastN > 0) {
			getSubAttributeInstancesQuery.setMaxResults(lastN);
		}
		return getSubAttributeInstancesQuery.getResultList();
	}

	private List<Attribute> findAttributeInstancesForEntity(String entityId, String attributeId, String timeQueryPart, String timeField, Integer lastN) {
		TypedQuery<Attribute> getAttributeInstancesQuery =
				entityManager.createQuery(
						"Select attribute " +
								"from Attribute attribute " +
								"where attribute.entityId=:entityId " +
								"and attribute.id=:attributeId " +
								"and attribute.opMode!='" + OpMode.Delete.name() + "' " +
								"and " + timeQueryPart + " " +
								"order by attribute.ts desc", Attribute.class);
		getAttributeInstancesQuery.setParameter("entityId", entityId);
		getAttributeInstancesQuery.setParameter("attributeId", attributeId);
		getAttributeInstancesQuery.setParameter("timeField", timeField);
		if (lastN != null && lastN > 0) {
			getAttributeInstancesQuery.setMaxResults(lastN);
		}
		return getAttributeInstancesQuery.getResultList();
	}

	private List<String> findAllAttributesForEntity(String entityId, String timeQueryPart, String timeField) {
		TypedQuery<String> getAttributeIdsQuery =
				entityManager.createQuery(
						"Select distinct attribute.id " +
								"from Attribute attribute " +
								"where attribute.entityId=:entityId " +
								"and attribute.opMode!='" + OpMode.Delete.name() + "' " +
								"and " + timeQueryPart, String.class);
		getAttributeIdsQuery.setParameter("timeField", timeField);
		getAttributeIdsQuery.setParameter("entityId", entityId);
		return getAttributeIdsQuery.getResultList();
	}


	private String getTimeQueryPart(TimeRelation timeRelation, Instant timeAt, Instant endTime) {
		InvalidTimeRelationException invalidTimeRelationException = new InvalidTimeRelationException("Received an invalid time relation.");
		if (timeRelation == null) {
			throw invalidTimeRelationException;
		}
		switch (timeRelation) {
			case BETWEEN:
				return String.format(":timeField < '%s' and :timeField > '%s'", endTime, timeAt);
			case BEFORE:
				return String.format(":timeField < '%s'", timeAt);
			case AFTER:
				return String.format(":timeField > '%s'", timeAt);
			default:
				throw invalidTimeRelationException;
		}
	}

	public List<Instant> getCreatedAtForAttribute(String attributeId, String entityId) {
		TypedQuery<Instant> instantTypedQuery = entityManager.
				createQuery(
						"select attribute.ts " +
								"from Attribute attribute " +
								"where attribute.entityId=:entityId and attribute.id=:attributeId and attribute.opMode='" + OpMode.Create.name() + "'" +
								" order by attribute.ts asc", Instant.class);
		instantTypedQuery.setParameter("entityId", entityId);
		instantTypedQuery.setParameter("attributeId", attributeId);

		List<Instant> createdInstants = instantTypedQuery.getResultList();
		return createdInstants;
	}

}
