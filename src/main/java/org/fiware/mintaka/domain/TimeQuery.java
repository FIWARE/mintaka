package org.fiware.mintaka.domain;

import lombok.Data;
import org.fiware.mintaka.exception.InvalidTimeRelationException;
import org.fiware.mintaka.exception.PersistenceRetrievalException;
import org.fiware.mintaka.persistence.OpMode;
import org.fiware.ngsi.model.TimerelVO;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Data
public class TimeQuery {

	private final TimeRelation timeRelation;
	private final Instant timeAt;
	private final Instant endTime;
	private final String timeProperty;

	public TimeQuery(TimeRelation timeRelation, Instant timeAt, Instant endTime, String timeProperty) {
		validateTimeRelation(timeAt, endTime, timeRelation);
		this.timeRelation = timeRelation;
		this.timeAt = timeAt;
		this.endTime = endTime;
		this.timeProperty = timeProperty;
	}

	/**
	 * Validate the given time relation combination. Throws an {@link InvalidTimeRelationException} if its not valid.
	 *
	 * @param time         timeReference as requested through the api
	 * @param endTime      endpoint of the requested timeframe
	 * @param timeRelation time relation as requested through the api
	 */
	private void validateTimeRelation(Instant time, Instant endTime, TimeRelation timeRelation) {
		if (timeRelation == null && time == null && endTime == null) {
			return;
		}
		if (timeRelation == null) {
			throw new InvalidTimeRelationException("Did not receive a valid time relation config.");
		}
		switch (timeRelation) {
			case AFTER:
				if (time != null && endTime == null) {
					return;
				}
			case BEFORE:
				if (time != null && endTime == null) {
					return;
				}
			case BETWEEN:
				if (time != null && endTime != null) {
					return;
				}
			default:
				throw new InvalidTimeRelationException("Did not receive a valid time relation config.");
		}
	}

	public String getSqlRepresentation() {
		return getSqlRepresentation(null);
	}

	public String getSqlRepresentation(String dbEntityType) {

		InvalidTimeRelationException invalidTimeRelationException = new InvalidTimeRelationException("Received an invalid time relation.");
		if (dbEntityType == null) {
			dbEntityType = "attribute";
		}

		if (timeRelation == null && timeAt == null && endTime == null) {
			return "";
		}
		String timePropertyQuery = "";
		if (timeProperty.equals("observedAt")) {
			timePropertyQuery += String.format("%s.observedAt", dbEntityType);
		} else if (timeProperty.equals("createdAt")) {
			timePropertyQuery += String.format(" %s.opMode='", dbEntityType) + OpMode.Create.name() + String.format("' and %s.ts", dbEntityType);
		} else if (timeProperty.equals("modifiedAt")) {
			timePropertyQuery += String.format(" %s.opMode!='", dbEntityType) + OpMode.Create.name() + String.format("' and %s.ts", dbEntityType);
		} else if (timeProperty.equals("ts")) {
			timePropertyQuery += String.format("%s.ts", dbEntityType);
		} else {
			throw new PersistenceRetrievalException(String.format("Querying by %s is currently not supported.", timeProperty));
		}

		LocalDateTime timeAtLDT = LocalDateTime.ofInstant(timeAt, ZoneOffset.UTC);

		switch (timeRelation) {
			case BETWEEN:
				return String.format("and %s > '%s' and  %s < '%s' ", timePropertyQuery, timeAtLDT, timePropertyQuery, LocalDateTime.ofInstant(endTime, ZoneOffset.UTC));
			case BEFORE:
				return String.format("and %s < '%s' ", timePropertyQuery, timeAtLDT);
			case AFTER:
				return String.format("and %s > '%s' ", timePropertyQuery, timeAtLDT);
			default:
				throw invalidTimeRelationException;
		}
	}
}
