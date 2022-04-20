/*
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */
package de.hybris.platform.outboundsync.config.impl;

import de.hybris.deltadetection.model.StreamConfigurationContainerModel;
import de.hybris.deltadetection.model.StreamConfigurationModel;
import de.hybris.platform.integrationservices.model.IntegrationObjectDescriptor;
import de.hybris.platform.integrationservices.model.IntegrationObjectItemModel;
import de.hybris.platform.integrationservices.model.IntegrationObjectModel;
import de.hybris.platform.integrationservices.model.TypeDescriptor;
import de.hybris.platform.integrationservices.model.impl.DefaultIntegrationObjectDescriptor;
import de.hybris.platform.integrationservices.model.impl.DescriptorUtils;
import de.hybris.platform.outboundsync.config.ChannelConfigurationFactory;
import de.hybris.platform.outboundsync.config.IdentifierGenerator;
import de.hybris.platform.outboundsync.config.StreamIdentifierGenerator;
import de.hybris.platform.outboundsync.model.OutboundChannelConfigurationModel;
import de.hybris.platform.outboundsync.model.OutboundSyncCronJobModel;
import de.hybris.platform.outboundsync.model.OutboundSyncJobModel;
import de.hybris.platform.outboundsync.model.OutboundSyncStreamConfigurationContainerModel;
import de.hybris.platform.outboundsync.model.OutboundSyncStreamConfigurationModel;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Required;

public class DefaultChannelConfigurationFactory implements ChannelConfigurationFactory
{
	private IdentifierGenerator<OutboundChannelConfigurationModel> streamContainerIdentifierGenerator;
	private StreamIdentifierGenerator streamIdentifierGenerator;
	private IdentifierGenerator<OutboundChannelConfigurationModel> jobIdentifierGenerator;
	private IdentifierGenerator<OutboundChannelConfigurationModel> cronJobIdentifierGenerator;

	@Override
	public OutboundSyncStreamConfigurationContainerModel createStreamContainer(final OutboundChannelConfigurationModel channel)
	{
		final OutboundSyncStreamConfigurationContainerModel container = new OutboundSyncStreamConfigurationContainerModel();
		container.setId(getStreamContainerIdentifierGenerator().generate(channel));
		return container;
	}

	@Override
	public List<StreamConfigurationModel> createStreams(final OutboundChannelConfigurationModel channel, final StreamConfigurationContainerModel streamContainer)
	{
		final IntegrationObjectModel io = channel.getIntegrationObject();
		if (io != null)
		{
			final IntegrationObjectItemModel rootItem = io.getRootItem();
			final Collection<IntegrationObjectItemModel> ioItems = rootItem != null
					? deriveItemsReferencingRootItem(io)
					: io.getItems();
			return ioItems.stream()
					.map(item -> createStreamConfig(item, channel, streamContainer))
					.collect(Collectors.toList());
		}
		return Collections.emptyList();
	}

	private List<IntegrationObjectItemModel> deriveItemsReferencingRootItem(final IntegrationObjectModel io)
	{
		final IntegrationObjectDescriptor ioDesc = createIntegrationObjectDescriptor(io);
		return ioDesc.getItemTypeDescriptors().stream()
				.filter(TypeDescriptor::hasPathToRoot)
				.map(DescriptorUtils::extractModelFrom)
				.filter(Optional::isPresent)
				.map(Optional::get)
				.collect(Collectors.toList());
	}

	/**
	 * Creates an integration object descriptor for the given integration object model.
	 * @param model a model to create a descriptor for
	 * @return a descriptor for the provided model.
	 * @throws IllegalArgumentException if the model is {@code null}.
	 */
	protected IntegrationObjectDescriptor createIntegrationObjectDescriptor(final IntegrationObjectModel model)
	{
		return DefaultIntegrationObjectDescriptor.create(model);
	}

	private OutboundSyncStreamConfigurationModel createStreamConfig(final IntegrationObjectItemModel item, final OutboundChannelConfigurationModel channel, final StreamConfigurationContainerModel container)
	{
		final OutboundSyncStreamConfigurationModel stream = new OutboundSyncStreamConfigurationModel();
		stream.setOutboundChannelConfiguration(channel);
		stream.setContainer(container);
		stream.setStreamId(getStreamIdentifierGenerator().generate(channel, item));
		stream.setItemTypeForStream(item.getType());
		stream.setActive(true);
		return stream;
	}

	@Override
	public OutboundSyncCronJobModel createCronJob(final OutboundChannelConfigurationModel channel, final OutboundSyncJobModel job)
	{
		final OutboundSyncCronJobModel cronJob = new OutboundSyncCronJobModel();
		cronJob.setCode(getCronJobIdentifierGenerator().generate(channel));
		cronJob.setJob(job);
		return cronJob;
	}

	@Override
	public OutboundSyncJobModel createJob(final OutboundChannelConfigurationModel channel, final OutboundSyncStreamConfigurationContainerModel streamConfigContainer)
	{
		final OutboundSyncJobModel job = new OutboundSyncJobModel();
		job.setCode(getJobIdentifierGenerator().generate(channel));
		job.setStreamConfigurationContainer(streamConfigContainer);
		return job;
	}

	protected StreamIdentifierGenerator getStreamIdentifierGenerator()
	{
		return streamIdentifierGenerator;
	}

	@Required
	public void setStreamIdentifierGenerator(final StreamIdentifierGenerator generator)
	{
		streamIdentifierGenerator = generator;
	}

	protected IdentifierGenerator<OutboundChannelConfigurationModel> getStreamContainerIdentifierGenerator()
	{
		return streamContainerIdentifierGenerator;
	}

	@Required
	public void setStreamContainerIdentifierGenerator(final IdentifierGenerator<OutboundChannelConfigurationModel> streamContainerIdentifierGenerator)
	{
		this.streamContainerIdentifierGenerator = streamContainerIdentifierGenerator;
	}

	protected IdentifierGenerator<OutboundChannelConfigurationModel> getJobIdentifierGenerator()
	{
		return jobIdentifierGenerator;
	}

	@Required
	public void setJobIdentifierGenerator(final IdentifierGenerator<OutboundChannelConfigurationModel> jobIdentifierGenerator)
	{
		this.jobIdentifierGenerator = jobIdentifierGenerator;
	}

	protected IdentifierGenerator<OutboundChannelConfigurationModel> getCronJobIdentifierGenerator()
	{
		return cronJobIdentifierGenerator;
	}

	@Required
	public void setCronJobIdentifierGenerator(final IdentifierGenerator<OutboundChannelConfigurationModel> cronJobIdentifierGenerator)
	{
		this.cronJobIdentifierGenerator = cronJobIdentifierGenerator;
	}
}
