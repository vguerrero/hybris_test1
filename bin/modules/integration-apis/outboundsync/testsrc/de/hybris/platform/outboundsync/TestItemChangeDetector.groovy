/*
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */

package de.hybris.platform.outboundsync

import de.hybris.deltadetection.ChangeDetectionService
import de.hybris.deltadetection.ItemChangeDTO
import de.hybris.deltadetection.StreamConfiguration
import de.hybris.platform.core.Registry
import de.hybris.platform.integrationservices.util.IntegrationTestUtil
import de.hybris.platform.outboundsync.job.impl.InMemoryGettableChangesCollector
import de.hybris.platform.outboundsync.model.OutboundChannelConfigurationModel
import de.hybris.platform.outboundsync.model.OutboundSyncStreamConfigurationModel
import org.junit.rules.ExternalResource

/**
 * A jUnit test rule for verifying item change conditions and creating/managing the change streams associated with
 * the items changes.
 */
class TestItemChangeDetector extends ExternalResource {
    private static final def STREAM_CONTAINER = 'outboundSyncDataStreams'

    private ChangeDetectionService deltaDetect
    Collection<OutboundSyncStreamConfigurationModel> allChangeStreams = []

    @Override
    protected void after() {
        reset()
    }

    /**
     * Resets its state by consuming all changes in all created change streams and deletes the created change streams.
     * This method does not affect streams created outside of this rule class.
     */
    void reset() {
        consumeAllCurrentChanges()
        IntegrationTestUtil.removeAll OutboundSyncStreamConfigurationModel
        allChangeStreams.clear()
    }

    /**
     * Consumes all changes, which possibly present in all so far created streams.
     * @see #createChangeStream(java.lang.String, java.lang.String)
     */
    void consumeAllCurrentChanges() {
        getDeltaDetect().consumeChanges getAllCurrentChanges()
    }

    /**
     * Determines whether any of the created change streams has unconsumed item changes.
     * @return {@code true}, if all changes are consumed; {@code false}, if at least one unconsumed item change is present in
     * at least of of the created streams.
     * @see #createChangeStream(java.lang.String, java.lang.String)
     * @see #getAllCurrentChanges()
     */
    boolean hasAllChangesConsumed() {
        getAllCurrentChanges().empty
    }

    /**
     * Retrieves all changes from all created change stream, which have not been consumed yet.
     * @return a list of item changes or an empty list, if there are no outstanding unconsumed changes.
     */
    List<ItemChangeDTO> getAllCurrentChanges() {
        allChangeStreams
                .collect { getChangesFromStream(it) }
                .flatten() as List<ItemChangeDTO>
    }

    private List<ItemChangeDTO> getChangesFromStream(OutboundSyncStreamConfigurationModel deltaStream) {
        def changesCollector = new InMemoryGettableChangesCollector()
        def configuration = StreamConfiguration.buildFor(deltaStream.getStreamId())
                .withItemSelector(deltaStream.getWhereClause())

        getDeltaDetect().collectChangesForType(deltaStream.getItemTypeForStream(), configuration, changesCollector)
        changesCollector.getChanges()
    }

    /**
     * Creates new delta detect stream and guarantees it has no outstanding changes in it.
     * @param channel a delta detect channel, the new stream should associated with.
     * @param typecode type code of a type system item, the stream should contain changes for.
     * @param filter additional {@code where} clause condition to be applied to the items in the stream. Only changes for the items
     * matching this conditions will be reported by the stream. Changes done to the non-matching items will no be contained in this stream.
     * @return the created change stream
     */
    OutboundSyncStreamConfigurationModel createChangeStream(OutboundChannelConfigurationModel channel, String typecode, String filter='') {
        createChangeStream channel.code, typecode, filter
    }

    /**
     * Creates new delta detect stream and guarantees it has no outstanding changes in it.
     * @param channelCode code of the corresponding delta detect channel, the new stream should associated with.
     * @param typecode type code of a type system item, the stream should contain changes for.
     * @return the created change stream
     */
    OutboundSyncStreamConfigurationModel createChangeStream(String channelCode, String typecode, String filter='') {
        OutboundSyncStreamConfigurationModel stream = persistStream(typecode, channelCode, filter)
        allChangeStreams << stream
        getDeltaDetect().consumeChanges getChangesFromStream(stream)
        return stream
    }

    private static OutboundSyncStreamConfigurationModel persistStream(String typecode, String channelCode, String filter) {
        def streamId = "${typecode}Stream"
        IntegrationTestUtil.importImpEx(
                "INSERT_UPDATE OutboundSyncStreamConfiguration; streamId[unique = true]; container(id)    ; itemTypeForStream(code); outboundChannelConfiguration(code); whereClause",
                "                                             ; $streamId              ; $STREAM_CONTAINER; $typecode              ; $channelCode                      ; $filter")
        IntegrationTestUtil.findAny(OutboundSyncStreamConfigurationModel, { it.streamId == streamId })
                .orElseThrow {
            new IllegalStateException("Failed to persist a ${typecode} change stream for channel ${channelCode}")
        }
    }


    private ChangeDetectionService getDeltaDetect() {
        if (!deltaDetect) {
            deltaDetect = Registry.getApplicationContext().getBean 'changeDetectionService', ChangeDetectionService
        }
        deltaDetect
    }
}