package org.fiware.mintaka.domain;

import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Optional;

/**
 * Pojo holding all pagination relevant informations.
 */
@RequiredArgsConstructor
@Getter
public class PaginationInformation {

	/**
	 * Current page size
	 */
	private final int pageSize;
	/**
	 * Id of the starting point for the previous page, empty if first page
	 */
	private final Optional<String> previousPage;
	/**
	 * Id of the starting point for the next page, empty if last page
	 */
	private final Optional<String> nextPage;
}
