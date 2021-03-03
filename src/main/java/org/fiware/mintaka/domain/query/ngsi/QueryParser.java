package org.fiware.mintaka.domain.query.ngsi;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.fiware.mintaka.context.LdContextCache;

import javax.inject.Singleton;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Parser for queries(according to the NGSI-LD api) into the internal QueryTerm representation.
 */
@Slf4j
@Singleton
@RequiredArgsConstructor
public class QueryParser {

	public static final char QUOTE = '"';
	private static final String STRING_VALUE_REGEX = "\".*?\"";
	public static final char OPEN_BRACE = '(';
	public static final char CLOSE_BRACE = ')';
	public static final char OPEN_SQUARE_BRACE = '[';
	public static final char CLOSE_SQUARE_BRACE = ']';

	private final LdContextCache contextCache;

	/**
	 * Translate a query into the QueryTerm representation, using the given context for expanding the attribute paths.
	 * @param query - string representation of the query
	 * @param contextURLs - url of the context's to use
	 * @return the query term
	 */
	public QueryTerm toTerm(String query, List<URL> contextURLs) {
		if (isLogicalQuery(query)) {
			return parseLogicalString(query, contextURLs);
		} else {
			return parseComparisonString(query, contextURLs);
		}
	}

	/**
	 * Check if the given string is a logical query
	 */
	private static boolean isLogicalQuery(String queryString) {
		String strippedString = stripStringValues(queryString);
		return Arrays.stream(LogicalOperator.values()).anyMatch(logicalOperator -> strippedString.contains(logicalOperator.getValue()));
	}

	/**
	 *
	 * Remove all string values from the given string(to ease the query checking).
	 */
	private static String stripStringValues(String queryString) {
		return queryString.replaceAll(STRING_VALUE_REGEX, "");
	}

	/**
	 * Parse the given comparison string into a query term
	 */
	private QueryTerm parseComparisonString(String comparisonString, List<URL> contextUrls) {
		return Arrays.stream(ComparisonOperator.values())
				.map(operator -> {
							// index of returns the first index, therefore no special handling for operators inside string values is required
							int operatorIndex = comparisonString.indexOf(operator.getValue());
							if (operatorIndex > 0) {
								String attributePath = comparisonString.substring(0, operatorIndex);
								String comparisonValue = comparisonString.substring(operatorIndex + operator.getValue().length());
								return new ComparisonTerm(comparisonString, operator, attributePath, comparisonValue, contextCache, contextUrls);
							}
							return null;
						}
				).filter(Objects::nonNull).findAny().orElseThrow(() -> new IllegalArgumentException(String.format("Comparison is not valid: %s", comparisonString)));
	}

	/**
	 * Parse a query string into a logical term
	 */
	private LogicalTerm parseLogicalString(String logicalString, List<URL> contextUrls) {
		LogicalTerm theTerm = new LogicalTerm(logicalString);
		logicalString = removeTopLevelBraces(logicalString);
		int openBraceCount = 0;
		boolean openQuote = false;
		//start index of the currently evaluated term. Should either be the start of the string or the position after the last logical operator.
		int currentTermStartIndex = 0;
		for (int i = 0; i < logicalString.length(); i++) {
			char currentChar = logicalString.charAt(i);
			if (isLogicalOperator(currentChar)) {
				if (isInsideSubTermOrStringValue(openBraceCount, openQuote)) {
					// cannot happen on top level, since it might hit on non logical operator chars
					continue;
				}

				// get the current subterm's string(ends one position before the operator)
				String subTerm = logicalString.substring(currentTermStartIndex, i);
				theTerm.addSubTerm(toTerm(subTerm, contextUrls));
				// add the operator before continuing with the next term
				theTerm.addSubTerm(new LogicalConnectionTerm(LogicalOperator.byCharValue(currentChar)));
				// new start index should be one position after the last operator
				currentTermStartIndex = i + 1;
				continue;
			}
			if (currentChar == OPEN_BRACE) {
				openBraceCount++;
				continue;
			}
			if (currentChar == CLOSE_BRACE) {
				openBraceCount--;
				continue;
			}
			if (currentChar == QUOTE) {
				openQuote = !openQuote;
			}
		}
		if (currentTermStartIndex == 0) {
			throw new IllegalArgumentException(String.format("A logical term should have at least two terms. Is: %s", logicalString));
		}
		// add the last sub term to the tree
		String subTerm = logicalString.substring(currentTermStartIndex);
		theTerm.addSubTerm(toTerm(subTerm, contextUrls));
		if (theTerm.getSubTerms().isEmpty()) {
			throw new IllegalArgumentException(String.format("A logical term needs to have at least one entry. Term: %s", logicalString));
		}
		return theTerm;
	}

	// check if the given char is a logical operator
	private static boolean isLogicalOperator(char charToCheck) {
		return charToCheck == LogicalOperator.AND.getCharValue() || charToCheck == LogicalOperator.OR.getCharValue();
	}

	// check if current position is inside a subterm or a string value
	private static boolean isInsideSubTermOrStringValue(int openBraceCount, boolean insideString) {
		return insideString || openBraceCount > 0;
	}

	/**
	 * Top level braces are redundant and can be removed before parsing, e.g. (a=b|c!=d) doesn't need braces.
	 *
	 * @return the string without toplevel braces.
	 */
	private static String removeTopLevelBraces(String queryString) {
		if (queryString.charAt(0) == OPEN_BRACE && queryString.charAt(queryString.length() - 1) == CLOSE_BRACE) {
			return queryString.substring(1, queryString.length() - 1);
		}
		return queryString;
	}
}
