/*
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */
package de.hybris.platform.outboundsync.job.impl;

import de.hybris.deltadetection.ChangeDetectionService;
import de.hybris.deltadetection.ItemChangeDTO;
import de.hybris.deltadetection.StreamConfiguration;
import de.hybris.platform.cronjob.enums.CronJobResult;
import de.hybris.platform.cronjob.enums.CronJobStatus;
import de.hybris.platform.cronjob.model.JobModel;
import de.hybris.platform.outboundsync.dto.OutboundItemDTO;
import de.hybris.platform.outboundsync.dto.impl.DeltaDetectionOutboundItemChange;
import de.hybris.platform.outboundsync.job.GettableChangesCollector;
import de.hybris.platform.outboundsync.job.ItemChangeSender;
import de.hybris.platform.outboundsync.model.OutboundSyncCronJobModel;
import de.hybris.platform.outboundsync.model.OutboundSyncJobModel;
import de.hybris.platform.outboundsync.model.OutboundSyncStreamConfigurationContainerModel;
import de.hybris.platform.outboundsync.model.OutboundSyncStreamConfigurationModel;
import de.hybris.platform.servicelayer.cronjob.AbstractJobPerformable;
import de.hybris.platform.servicelayer.cronjob.JobPerformable;
import de.hybris.platform.servicelayer.cronjob.PerformResult;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * This {@link JobPerformable} collects
 * the changes specified in the {@link StreamConfiguration} and send them out
 * via the {@link ItemChangeSender}.
 */
public class OutboundSyncCronJobPerformable extends AbstractJobPerformable<OutboundSyncCronJobModel>
{
	private static final Logger LOGGER = LoggerFactory.getLogger(OutboundSyncCronJobPerformable.class);

	private ChangeDetectionService changeDetectionService;
	private ItemChangeSender itemChangeSender;
	private GettableChangesCollectorProvider changesCollectorProvider;

	@Override
	public PerformResult perform(final OutboundSyncCronJobModel cronJob)
	{
		final JobModel job = cronJob.getJob();

		final String reason;
		if (job instanceof OutboundSyncJobModel)
		{
			final OutboundSyncJobModel outboundSyncJob = (OutboundSyncJobModel) job;
			final OutboundSyncStreamConfigurationContainerModel streamConfigurationContainer = outboundSyncJob.getStreamConfigurationContainer();

			if (streamConfigurationContainer != null)
			{
				try
				{
					LOGGER.debug("Collecting and sending changes for each configuration");
					final List<OutboundItemDTO> changes = collectChangesFromConfigurations(streamConfigurationContainer);
					changes.forEach(getItemChangeSender()::send);
				}
				catch (final RuntimeException e)
				{
					LOGGER.error("Error occurred while running job {} with stream configuration container (id: {}).", job.getCode(), streamConfigurationContainer.getId(), e);
					return new PerformResult(CronJobResult.FAILURE, CronJobStatus.FINISHED);
				}
				return new PerformResult(CronJobResult.SUCCESS, CronJobStatus.FINISHED);
			}
			else
			{
				reason = "Stream configuration container is null";
			}
		}
		else
		{
			reason = "Job is not an instance of OutboundSyncJobModel";
		}

		LOGGER.warn("Can't perform job because {}, marking job result as ERROR", reason);
		return new PerformResult(CronJobResult.ERROR, CronJobStatus.FINISHED);
	}

	/**
	 * Collect changes from all the stream configurations
	 *
	 * @param streamConfigurationContainer Container with all the stream configurations
	 * @return Changes from all the stream configurations
	 */
	protected List<OutboundItemDTO> collectChangesFromConfigurations(final OutboundSyncStreamConfigurationContainerModel streamConfigurationContainer)
	{
		return streamConfigurationContainer.getConfigurations()
				.stream()
				.map(s -> (OutboundSyncStreamConfigurationModel) s)
				.flatMap(this::collectChanges)
				.collect(Collectors.toList());
	}

	/**
	 * Collects the changes from the given stream configuration
	 *
	 * @param deltaStream Collect the changes for this stream configuration
	 * @return List of changes
	 */
	protected Stream<OutboundItemDTO> collectChanges(final OutboundSyncStreamConfigurationModel deltaStream)
	{
		final StreamConfiguration configuration = getStreamConfiguration(deltaStream);
		final GettableChangesCollector changesCollector = getGettableChangesCollectorProvider().getCollector();

		LOGGER.debug("Collecting changes for stream '{}'", deltaStream.getStreamId());
		getChangeDetectionService().collectChangesForType(
				deltaStream.getItemTypeForStream(),
				configuration,
				changesCollector);

		final List<ItemChangeDTO> changes = changesCollector.getChanges();
		return changes.stream()
				.map(change -> OutboundItemDTO.Builder.item()
						.withItem(new DeltaDetectionOutboundItemChange(change))
						.withIntegrationObjectPK(deltaStream.getOutboundChannelConfiguration().getIntegrationObject().getPk().getLong())
						.withChannelConfigurationPK(deltaStream.getOutboundChannelConfiguration().getPk().getLong())
						.build());
	}

	protected StreamConfiguration getStreamConfiguration(final OutboundSyncStreamConfigurationModel deltaStream)
	{
		return StreamConfiguration.buildFor(deltaStream.getStreamId())
				.withItemSelector(deltaStream.getWhereClause());
	}

	protected ChangeDetectionService getChangeDetectionService()
	{
		return changeDetectionService;
	}

	@Required
	public void setChangeDetectionService(final ChangeDetectionService changeDetectionService)
	{
		this.changeDetectionService = changeDetectionService;
	}

	protected ItemChangeSender getItemChangeSender()
	{
		return itemChangeSender;
	}

	@Required
	public void setItemChangeSender(final ItemChangeSender itemChangeSender)
	{
		this.itemChangeSender = itemChangeSender;
	}

	protected GettableChangesCollectorProvider getGettableChangesCollectorProvider()
	{
		return changesCollectorProvider;
	}

	@Required
	public void setGettableChangesCollectorProvider(final GettableChangesCollectorProvider changesCollectorProvider)
	{
		this.changesCollectorProvider = changesCollectorProvider;
	}
}
