package org.fiware.mintaka.domain.query.temporal;

import lombok.Data;
import org.fiware.mintaka.exception.InvalidTimeRelationException;
import org.fiware.mintaka.exception.PersistenceRetrievalException;
import org.fiware.mintaka.persistence.OpMode;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Data
public class TimeQuery {

	private final TimeRelation timeRelation;
	private final Instant timeAt;
	private final Instant endTime;
	private final String timeProperty;
	private final boolean inclusive;

	public TimeQuery(TimeRelation timeRelation, Instant timeAt, Instant endTime, String timeProperty) {
		this(timeRelation, timeAt, endTime, timeProperty, false, true);
	}

	public TimeQuery(TimeRelation timeRelation, Instant timeAt, Instant endTime, String timeProperty, boolean retrieval) {
		this(timeRelation, timeAt, endTime, timeProperty, false, retrieval);
	}

	public TimeQuery(TimeRelation timeRelation, Instant timeAt, Instant endTime, String timeProperty, boolean inclusive, boolean retrieval) {
		validateTimeRelation(timeAt, endTime, timeRelation, retrieval);
		this.timeRelation = timeRelation;
		this.timeAt = timeAt;
		this.endTime = endTime;
		this.timeProperty = timeProperty;
		this.inclusive = inclusive;
	}



	/**
	 * Validate the given time relation combination. Throws an {@link InvalidTimeRelationException} if its not valid.
	 *
	 * @param time         timeReference as requested through the api
	 * @param endTime      endpoint of the requested timeframe
	 * @param timeRelation time relation as requested through the api
	 * @param  retrieval empty timeRel is only allowed for retrieval, so indicate if we validate a retrieval
	 */
	private void validateTimeRelation(Instant time, Instant endTime, TimeRelation timeRelation, boolean retrieval) {
		if (timeRelation == null && time == null && endTime == null && retrieval) {
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

		String timePropertyQuery = "";
		if (TimeStampType.OBSERVED_AT.value().equals(timeProperty)) {
			timePropertyQuery += String.format("%s.observedAt", dbEntityType);
		} else if (TimeStampType.CREATED_AT.value().equals(timeProperty)) {
			timePropertyQuery += String.format(" %s.opMode='", dbEntityType) + OpMode.Create.name() + String.format("' and %s.ts", dbEntityType);
		} else if (TimeStampType.MODIFIED_AT.value().equals(timeProperty)) {
			timePropertyQuery += String.format("%s.ts", dbEntityType);
		} else if (TimeStampType.TS.value().equals(timeProperty)) {
			timePropertyQuery += String.format("%s.ts", dbEntityType);
		} else {
			throw new PersistenceRetrievalException(String.format("Querying by %s is currently not supported.", timeProperty));
		}

		if (timeRelation == null && timeAt == null && endTime == null) {
			return String.format("and %s IS NOT NULL ", timePropertyQuery);
		}

		LocalDateTime timeAtLDT = LocalDateTime.ofInstant(timeAt, ZoneOffset.UTC);

		switch (timeRelation) {
			case BETWEEN:
				if (inclusive) {
					return String.format("and %s >= '%s' and  %s <= '%s' ", timePropertyQuery, timeAtLDT, timePropertyQuery, LocalDateTime.ofInstant(endTime, ZoneOffset.UTC));
				}
				return String.format("and %s > '%s' and  %s < '%s' ", timePropertyQuery, timeAtLDT, timePropertyQuery, LocalDateTime.ofInstant(endTime, ZoneOffset.UTC));
			case BEFORE:
				if (inclusive) {
					return String.format("and %s <= '%s' ", timePropertyQuery, timeAtLDT);
				}
				return String.format("and %s < '%s' ", timePropertyQuery, timeAtLDT);
			case AFTER:
				if (inclusive) {
					return String.format("and %s >= '%s' ", timePropertyQuery, timeAtLDT);
				}
				return String.format("and %s > '%s' ", timePropertyQuery, timeAtLDT);
			default:
				throw invalidTimeRelationException;
		}
	}

	public String getDBTimeField() {
		if(timeProperty.equals(TimeStampType.OBSERVED_AT.value())) {
			return TimeStampType.OBSERVED_AT.value();
		}
		return "ts";
	}

	public TimeStampType getTimeStampType() {
		return TimeStampType.getEnum(timeProperty);
	}
}
