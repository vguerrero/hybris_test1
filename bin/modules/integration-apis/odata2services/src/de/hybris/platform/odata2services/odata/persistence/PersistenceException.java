/*
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */

package de.hybris.platform.odata2services.odata.persistence;

import org.apache.olingo.odata2.api.commons.HttpStatusCodes;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistenceException extends PersistenceRuntimeApplicationException
{
	private static final HttpStatusCodes STATUS_CODE = HttpStatusCodes.INTERNAL_SERVER_ERROR;
	private static final String DEFAULT_ERROR_CODE = "internal_error";
	private static final Logger LOGGER = LoggerFactory.getLogger(PersistenceException.class);

	/**
	 * Constructor to create PersistenceException
	 *
	 * @param e exception that was thrown
	 * @param storageRequest object that holds values for creating or updating an item
	 */
	public PersistenceException(final Throwable e, final StorageRequest storageRequest)
	{
		super(generateMessage(storageRequest), STATUS_CODE, DEFAULT_ERROR_CODE, e, storageRequest.getIntegrationKey());
	}

	private static String generateMessage(final StorageRequest storageRequest)
	{
		try
		{
			return String.format("An error occurred while attempting to save entries for entityType: %s.", storageRequest.getEntityType().getName());
		}
		catch (final EdmException e)
		{
			LOGGER.error("An EdmException occurred while attempting to get the EntityType name", e);
			return "An error occurred while attempting to save entries.";
		}
	}
}
