package org.fiware.mintaka.domain.query;

import lombok.Getter;
import lombok.ToString;
import org.fiware.mintaka.context.LdContextCache;

import java.net.URL;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.fiware.mintaka.rest.TemporalApiController.WELL_KNOWN_ATTRIBUTES;

@Getter
@ToString
public class ComparisonTerm extends QueryTerm {

	private static final char DOT_SEPERATOR = '.';
	private static final String DOT_SEPERATOR_STRING = String.valueOf(DOT_SEPERATOR);
	private static final String ESCAPED_DOT_SEPERATOR_STRING = "\\.";
	private static final String RANGE_SEPERATOR = "..";
	private static final String ESCAPED_RANGE_SEPERATOR = "\\.\\.";
	private static final char LEFT_SQUARE_BRACKET = '[';
	private static final String LEFT_SQUARE_BRACKET_STRING = String.valueOf(LEFT_SQUARE_BRACKET);
	private static final char RIGHT_SQUARE_BRACKET = ']';
	private static final String RIGHT_SQUARE_BRACKET_STRING = String.valueOf(RIGHT_SQUARE_BRACKET);

	private static final String OBSERVED_AT = "observedAt";
	private static final String DATA_SET_ID = "datasetId";
	private static final String MODIFIED_AT = "modifiedAt";
	private static final String CREATED_AT = "createdAt";
	private static final String UNIT_CODE = "unitCode";

	private static final DateFormat TIME_FORMAT = new SimpleDateFormat("hh:mm:ss,ssssssZ");
	private static final DateFormat DATE_FORMAT = new SimpleDateFormat("YYYY-MM-DD");

	private final ComparisonOperator operator;
	private final String attributePath;
	private final String comparisonValue;
	private final LdContextCache contextCache;
	private final List<URL> contextUrls;

	public ComparisonTerm(String term, ComparisonOperator operator, String attributePath, String comparisonValue, LdContextCache contextCache, List<URL> contextUrls) {
		super(term);
		this.operator = operator;
		this.attributePath = attributePath;
		this.comparisonValue = comparisonValue;
		this.contextCache = contextCache;
		this.contextUrls = contextUrls;
	}

	@Override
	public String toSQLQuery() {
		String selectionQuery = getSelectionQuery();

		Optional<String> optionalSubAttributePath = getSubAttributePath();
		if (optionalSubAttributePath.isPresent()) {
			return String.format(" instanceid in (SELECT attrinstanceid  FROM subattributes sa WHERE %s and id='%s')", selectionQuery, optionalSubAttributePath.get());
		}
		return getSelectionQuery();
	}

	public String getAttributePath() {
		if (attributePath.contains(DOT_SEPERATOR_STRING)) {
			return expandAttribute(attributePath.split(ESCAPED_DOT_SEPERATOR_STRING)[0]);
		}
		return expandAttribute(attributePath);
	}

	private String getSelectionQuery() {
		Optional<String> optionalWellKnownQuery = getWellKnownQuery();
		if (optionalWellKnownQuery.isPresent()) {
			return optionalWellKnownQuery.get();
		}
		Optional<String> optionalCompoundQuery = getCompoundQuery();
		if (optionalCompoundQuery.isPresent()) {
			// query inside a compound
			return optionalCompoundQuery.get();
		}
		Optional<String> optionalListQuery = getListQuery();
		if (optionalListQuery.isPresent()) {
			// query for list
			return optionalListQuery.get();
		}
		Optional<String> optionalRangeQuery = getRangeQuery();
		if (optionalRangeQuery.isPresent()) {
			// query for range
			return optionalRangeQuery.get();
		}
		Optional<String> optionalStringValue = getStringValue();
		if (optionalStringValue.isPresent()) {
			// query for string
			return String.format(" text%s'%s'", operator.getDbOperator(), optionalStringValue.get());
		}
		Optional<Number> optionalNumberValue = getNumberValue();
		if (optionalNumberValue.isPresent()) {
			// query for number
			return String.format(" number%s%s", operator.getDbOperator(), optionalNumberValue.get());
		}
		Optional<String> optionalTimeValue = getTimeValue();
		if (optionalTimeValue.isPresent()) {
			// query for time
			return String.format(" datetime::time%s'%s'::time", operator.getDbOperator(), optionalTimeValue.get());
		}
		Optional<String> optionalDateValue = getDateValue();
		if (optionalDateValue.isPresent()) {
			// query for date
			return String.format(" datetime%s'%s'", operator.getDbOperator(), optionalDateValue.get());
		}
		Optional<String> optionalDateTimeValue = getDateTimeValue();
		if (optionalDateTimeValue.isPresent()) {
			// query for datetime
			return String.format(" datetime%s'%s'", operator.getDbOperator(), optionalDateTimeValue.get());
		}
		Optional<Boolean> optionalBooleanValue = getBooleanValue();
		if (optionalBooleanValue.isPresent()) {
			return String.format(" boolean%s%s", operator.getDbOperator(), optionalBooleanValue.get());
		}
		throw new IllegalArgumentException(String.format("Comparison with the given value is not supported. Value: %s", comparisonValue));
	}

	private Optional<String> getSubAttributePath() {
		if (!attributePath.contains(DOT_SEPERATOR_STRING)) {
			return Optional.empty();
		}
		String[] pathComponents = attributePath.split(ESCAPED_DOT_SEPERATOR_STRING);
		Optional<String> optionalCompoundQuery = getCompoundQuery(pathComponents[1]);
		if (optionalCompoundQuery.isPresent()) {
			return Optional.empty();
		}
		return Optional.of(expandAttribute(pathComponents[1]));
	}

	private String expandAttribute(String attribute) {
		if (WELL_KNOWN_ATTRIBUTES.contains(attribute)) {
			return attribute;
		} else {
			// path needs to be expanded
			return contextCache.expandString(attribute, contextUrls);
		}
	}

	private Optional<String> getWellKnownQuery() {
		if (attributePath.equals(OBSERVED_AT) || attributePath.equals(CREATED_AT) || attributePath.equals(MODIFIED_AT)) {
			Optional<String> optionalTimeValue = getTimeValue();
			if (optionalTimeValue.isPresent()) {
				return Optional.of(String.format(" %s::time%s'%s'::time", attributePath, operator.getDbOperator(), optionalTimeValue.get()));
			}
			Optional<String> optionalDateTimeValue = getDateTimeValue();
			if (optionalDateTimeValue.isPresent()) {
				return Optional.of(String.format(" %s:%s'%s'", attributePath, operator.getDbOperator(), optionalDateTimeValue.get()));

			}
			Optional<String> optionalDateValue = getDateValue();
			if (optionalDateValue.isPresent()) {
				return Optional.of(String.format(" %s:%s'%s'", attributePath, operator.getDbOperator(), optionalDateValue.get()));
			}
		}
		if (attributePath.equals(UNIT_CODE) || attributePath.equals(DATA_SET_ID)) {
			return Optional.of(String.format(" %s:%s'%s'", attributePath, operator.getDbOperator(), comparisonValue));
		}
		return Optional.empty();
	}

	private Optional<String> getCompoundQuery() {
		return getCompoundQuery(attributePath);
	}

	private Optional<String> getCompoundQuery(String attributePath) {
		if (!attributePath.contains(LEFT_SQUARE_BRACKET_STRING) || !attributePath.contains(RIGHT_SQUARE_BRACKET_STRING)) {
			return Optional.empty();
		}
		int leftIndex = attributePath.indexOf(LEFT_SQUARE_BRACKET_STRING);
		int rightIndex = attributePath.indexOf(RIGHT_SQUARE_BRACKET_STRING);
		String jsonKey = attributePath.substring(leftIndex + 1, rightIndex);
		return Optional.of(String.format(" compound -> '%s' %s '%s'", jsonKey, operator.getDbOperator(), comparisonValue));
	}

	private Optional<String> getRangeQuery() {
		IllegalArgumentException exception = new IllegalArgumentException(String.format("%s is not a valid range.", comparisonValue));
		if (!getStringValue().isEmpty() || !comparisonValue.contains(RANGE_SEPERATOR)) {
			return Optional.empty();
		}
		String[] rangeValue = comparisonValue.split(ESCAPED_RANGE_SEPERATOR);
		if (rangeValue.length != 2) {
			throw exception;
		}

		// kind of code duplication is required to ensure early exit.

		Optional<String> optionalStringValueMin = getStringValue(rangeValue[0]);
		Optional<String> optionalStringValueMax = getStringValue(rangeValue[1]);
		if (optionalStringValueMin.isPresent() && optionalStringValueMax.isPresent()) {
			// query for string
			return Optional.of(String.format(" text BETWEEN '%s' AND '%s'", optionalStringValueMin.get(), optionalStringValueMax.get()));
		}
		Optional<Number> optionalNumberValueMin = getNumberValue(rangeValue[0]);
		Optional<Number> optionalNumberValueMax = getNumberValue(rangeValue[1]);
		if (optionalNumberValueMin.isPresent() && optionalNumberValueMax.isPresent()) {
			// query for number
			return Optional.of(String.format(" number BETWEEN %s AND %s", optionalNumberValueMin.get(), optionalNumberValueMax.get()));
		}
		Optional<String> optionalTimeValueMin = getTimeValue(rangeValue[0]);
		Optional<String> optionalTimeValueMax = getTimeValue(rangeValue[0]);
		if (optionalTimeValueMin.isPresent() && optionalTimeValueMax.isPresent()) {
			// query for time
			return Optional.of(String.format(" datetime::time BETWEEN %s::time AND %s::time", optionalTimeValueMin.get(), optionalTimeValueMax.get()));
		}
		Optional<String> optionalDateValueMin = getDateValue(rangeValue[0]);
		Optional<String> optionalDateValueMax = getDateValue(rangeValue[1]);
		if (optionalDateValueMin.isPresent() && optionalDateValueMax.isPresent()) {
			// query for date
			return Optional.of(String.format(" datetime BETWEEN %s AND %s", optionalDateValueMin.get(), optionalDateValueMax.get()));
		}
		Optional<String> optionalDateTimeValueMin = getDateTimeValue(rangeValue[0]);
		Optional<String> optionalDateTimeValueMax = getDateTimeValue(rangeValue[0]);
		if (optionalDateTimeValueMin.isPresent() && optionalDateTimeValueMax.isPresent()) {
			// query for datetime
			return Optional.of(String.format(" datetime BETWEEN %s AND %s", optionalDateTimeValueMin.get(), optionalDateTimeValueMax.get()));
		}
		throw exception;
	}

	private Optional<String> getListQuery() {
		boolean isList = comparisonValue.charAt(0) == QueryParser.OPEN_BRACE && comparisonValue.charAt(comparisonValue.length() - 1) == QueryParser.CLOSE_BRACE;
		if (!isList) {
			return Optional.empty();
		}
		return Optional.of(
				"boolean IN " + comparisonValue + " " +
						"OR text IN " + comparisonValue + " " +
						"OR number IN " + comparisonValue + " " +
						"OR datetime IN " + comparisonValue);
	}

	private Optional<String> getStringValue(String toCheck) {
		if (toCheck.charAt(0) == QueryParser.QUOTE && toCheck.charAt(toCheck.length() - 1) == QueryParser.QUOTE) {
			// return everything except the quotes
			return Optional.of(toCheck.substring(1, toCheck.length() - 1));
		}
		return Optional.empty();
	}

	private Optional<String> getStringValue() {
		return getStringValue(comparisonValue);
	}

	private Optional<Number> getNumberValue(String toCheck) {
		try {
			return Optional.of(NumberFormat.getInstance().parse(toCheck));
		} catch (ParseException e) {
			return Optional.empty();
		}
	}

	private Optional<Number> getNumberValue() {
		return getNumberValue(comparisonValue);
	}

	private Optional<String> getTimeValue(String toCheck) {
		try {
			TIME_FORMAT.parse(toCheck);
			// postgres supports the same time format as ngsi-ld, except that it uses . for ms separation instead of ,
			return Optional.of(toCheck.replace(',', '.'));
		} catch (ParseException e) {
			return Optional.empty();
		}
	}

	private Optional<String> getTimeValue() {
		return getTimeValue(comparisonValue);
	}

	private Optional<String> getDateValue(String toCheck) {
		try {
			DATE_FORMAT.parse(toCheck);
			return Optional.of(toCheck);
		} catch (ParseException e) {
			return Optional.empty();
		}
	}

	private Optional<String> getDateValue() {
		return getDateValue(comparisonValue);
	}

	private Optional<String> getDateTimeValue(String toCheck) {
		try {
			if (LocalDateTime.parse(toCheck) != null) {
				return Optional.of(toCheck.replace(',', '.'));
			}
			return Optional.empty();
		} catch (RuntimeException e) {
			return Optional.empty();
		}
	}

	private Optional<String> getDateTimeValue() {
		return getDateTimeValue(comparisonValue);
	}

	private Optional<Boolean> getBooleanValue(String toCheck) {
		if (toCheck.equals("true")) {
			return Optional.of(true);
		}
		if (toCheck.equals("false")) {
			return Optional.of(false);
		}
		return Optional.empty();
	}

	private Optional<Boolean> getBooleanValue() {
		return getBooleanValue(comparisonValue);

	}
}
