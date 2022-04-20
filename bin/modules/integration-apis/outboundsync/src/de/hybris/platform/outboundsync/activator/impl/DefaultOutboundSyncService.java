/*
 * [y] hybris Platform
 *
 * Copyright (c) 2019 SAP SE or an SAP affiliate company.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of SAP
 * ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the
 * license agreement you entered into with SAP.
 */
package de.hybris.platform.outboundsync.activator.impl;

import de.hybris.platform.core.PK;
import de.hybris.platform.core.model.ItemModel;
import de.hybris.platform.outboundservices.facade.OutboundServiceFacade;
import de.hybris.platform.outboundsync.activator.OutboundItemConsumer;
import de.hybris.platform.outboundsync.activator.OutboundSyncService;
import de.hybris.platform.outboundsync.dto.OutboundItemDTO;
import de.hybris.platform.outboundsync.dto.OutboundItemDTOGroup;
import de.hybris.platform.outboundsync.job.OutboundItemFactory;
import de.hybris.platform.outboundsync.retry.RetryUpdateException;
import de.hybris.platform.outboundsync.retry.SyncRetryService;
import de.hybris.platform.servicelayer.exceptions.ModelLoadingException;
import de.hybris.platform.servicelayer.model.ModelService;

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import rx.Observable;

/**
 * Default implementation of {@link OutboundSyncService} that uses {@link OutboundServiceFacade} for sending changes to the
 * destinations.
 */
public class DefaultOutboundSyncService implements OutboundSyncService
{
	private static final Logger LOG = LoggerFactory.getLogger(DefaultOutboundSyncService.class);

	private ModelService modelService;
	private OutboundItemFactory outboundItemFactory;
	private OutboundServiceFacade outboundServiceFacade;
	private OutboundItemConsumer outboundItemConsumer;
	private SyncRetryService syncRetryService;

	@Override
	public void sync(final Collection<OutboundItemDTO> outboundItemDTOs)
	{
		final OutboundItemDTOGroup outboundItemDTOGroup = OutboundItemDTOGroup.from(outboundItemDTOs, getOutboundItemFactory());
		final Long rootItemPk = outboundItemDTOGroup.getRootItemPk();

		LOG.debug("Synchronizing changes in item with PK={}", rootItemPk);
		final ItemModel itemModel = findItemByPk(rootItemPk);
		if (itemModel != null)
		{
			final String integrationObjectCode = outboundItemDTOGroup.getIntegrationObjectCode();
			final String destinationCode = outboundItemDTOGroup.getDestinationId();
			final Observable<ResponseEntity<Map>> outboundResponse =
					getOutboundServiceFacade().send(itemModel, integrationObjectCode, destinationCode);
			outboundResponse.subscribe(r -> handleResponse(r, outboundItemDTOGroup), e -> handleError(e, outboundItemDTOGroup));
		}
	}

	protected void handleError(final Throwable throwable, final OutboundItemDTOGroup outboundItemDTOGroup)
	{
		LOG.error("Failed to send item with PK={}", outboundItemDTOGroup.getRootItemPk(), throwable);
		handleError(outboundItemDTOGroup);
	}

	protected void handleError(final OutboundItemDTOGroup outboundItemDTOGroup)
	{
		LOG.warn("The item with PK={} could't be synchronized", outboundItemDTOGroup.getRootItemPk());

		try
		{
			if (getSyncRetryService().handleSyncFailure(outboundItemDTOGroup))
			{
				consumeChanges(outboundItemDTOGroup);
			}
		}
		// Due to the observable.onerror flow, we'll never get to this catch block. The plan is to get rid of the Observable in
		// the facade invocation, so this code block will then be correct
		catch (final RetryUpdateException e)
		{
			LOG.debug("Retry could not be updated", e);
		}
	}

	protected void handleResponse(final ResponseEntity<Map> responseEntity, final OutboundItemDTOGroup outboundItemDTOGroup)
	{
		if (responseEntity.getStatusCode() == HttpStatus.CREATED)
		{
			handleSuccessfulSync(outboundItemDTOGroup);
		}
		else
		{
			handleError(outboundItemDTOGroup);
		}
	}

	protected ItemModel findItemByPk(final Long pk)
	{
		try
		{
			if (pk != null)
			{
				return getModelService().get(PK.fromLong(pk));
			}
			LOG.debug("Not finding the item because the PK provided is null");
		}
		catch (final ModelLoadingException e)
		{
			LOG.warn("The item with PK={} was not found. Caused by {}", pk, e);
		}
		return null;
	}

	protected void handleSuccessfulSync(final OutboundItemDTOGroup outboundItemDTOGroup)
	{
		LOG.debug("The product with PK={} has been synchronized", outboundItemDTOGroup.getRootItemPk());
		try
		{
			getSyncRetryService().handleSyncSuccess(outboundItemDTOGroup);
			consumeChanges(outboundItemDTOGroup);
		}
		catch (final RetryUpdateException e)
		{
			LOG.debug("Retry could not be updated", e);
		}
	}

	protected void consumeChanges(final OutboundItemDTOGroup outboundItemDTOGroup)
	{
		outboundItemDTOGroup.getOutboundItemDTOs().forEach(getOutboundItemConsumer()::consume);
	}

	protected ModelService getModelService()
	{
		return modelService;
	}

	@Required
	public void setModelService(final ModelService modelService)
	{
		this.modelService = modelService;
	}

	protected OutboundItemFactory getOutboundItemFactory()
	{
		return outboundItemFactory;
	}

	@Required
	public void setOutboundItemFactory(final OutboundItemFactory factory)
	{
		outboundItemFactory = factory;
	}

	protected OutboundServiceFacade getOutboundServiceFacade()
	{
		return outboundServiceFacade;
	}

	@Required
	public void setOutboundServiceFacade(final OutboundServiceFacade outboundServiceFacade)
	{
		this.outboundServiceFacade = outboundServiceFacade;
	}

	protected OutboundItemConsumer getOutboundItemConsumer()
	{
		return outboundItemConsumer;
	}

	@Required
	public void setOutboundItemConsumer(final OutboundItemConsumer outboundItemConsumer)
	{
		this.outboundItemConsumer = outboundItemConsumer;
	}

	protected SyncRetryService getSyncRetryService()
	{
		return syncRetryService;
	}

	@Required
	public void setSyncRetryService(final SyncRetryService syncRetryService)
	{
		this.syncRetryService = syncRetryService;
	}
}

