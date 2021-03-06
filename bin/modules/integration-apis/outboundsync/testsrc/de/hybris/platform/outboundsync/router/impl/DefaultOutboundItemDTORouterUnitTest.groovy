/*
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */

package de.hybris.platform.outboundsync.router.impl

import de.hybris.bootstrap.annotations.UnitTest
import de.hybris.platform.outboundsync.activator.OutboundItemConsumer
import de.hybris.platform.outboundsync.dto.OutboundChangeType
import de.hybris.platform.outboundsync.dto.OutboundItemChange
import de.hybris.platform.outboundsync.dto.OutboundItemDTO
import de.hybris.platform.outboundsync.job.RootItemChangeSender
import org.junit.Test
import spock.lang.Specification

@UnitTest
class DefaultOutboundItemDTORouterUnitTest extends Specification {
	private static final Long ROOT_ITEM_PK = 1L
	private static final Long NON_ROOT_ITEM_PK = 2L
	private static final Long DELETED_ITEM_PK = 3L
	private static final Long REFERENCE_TO_ROOT_PK = 4L
	private static final Long MULTIPLE_REFERENCE_TO_ROOT_PK = 5L

	def router = new DefaultOutboundItemDTORouter()
	def changeConsumer = Mock(OutboundItemConsumer)
	def itemSender = Mock(RootItemChangeSender)
	def populator = Stub(RootItemPKPopulator)

	def setup() {
		router.setRootItemChangeSender(itemSender)
		router.setChangeConsumer(changeConsumer)
		router.setPopulator(populator)
	}

	@Test
	def "deleted items should be routed to the item change consumer"() {
		when:
		router.route(deleteDto())

		then:
		1 * changeConsumer.consume({ it.item.getPK() == DELETED_ITEM_PK })
	}

	@Test
	def "item that is not the root and does not have an attribute reference to the root is routed to change consumer"() {
		given:
		def nonRootItem = dtoWithPk(NON_ROOT_ITEM_PK)
		and:
		populator.populatePK(nonRootItem) >> [nonRootItem]

		when:
		router.route(nonRootItem)

		then:
		1 * changeConsumer.consume({ it.item.getPK() == NON_ROOT_ITEM_PK })
	}

	@Test
	def "root item is routed to item sender"() {
		given:
		def rootItem = dtoWithReferenceToRoot(ROOT_ITEM_PK)
		and:
		populator.populatePK(rootItem) >> [rootItem]

		when:
		router.route(rootItem)

		then:
		1 * itemSender.sendPopulatedItem({ it.getRootItemPK() == ROOT_ITEM_PK })
	}

	@Test
	def "item that is not the root but has a reference to the root is routed to item sender"() {
		given:
		def nonRootItem = dtoWithReferenceToRoot(REFERENCE_TO_ROOT_PK)
		and:
		populator.populatePK(nonRootItem) >> [nonRootItem]

		when:
		router.route(nonRootItem)

		then:
		1 * itemSender.sendPopulatedItem({ it.item.getPK() == REFERENCE_TO_ROOT_PK })
	}

	@Test
	def "item that is not the root and has a multiple references to the root is routed to item sender"() {
		given:
		def nonRootItem = dtoWithReferenceToRoot(MULTIPLE_REFERENCE_TO_ROOT_PK)
		and:
		populator.populatePK(nonRootItem) >> [nonRootItem, nonRootItem, nonRootItem]

		when:
		router.route(nonRootItem)

		then:
		3 * itemSender.sendPopulatedItem({ it.item.getPK() == MULTIPLE_REFERENCE_TO_ROOT_PK })
	}

	private def deleteDto() {
		Stub(OutboundItemDTO) {
			getItem() >> Stub(OutboundItemChange) {
				getChangeType() >> OutboundChangeType.DELETED
				getPK() >> DELETED_ITEM_PK
			}
		}
	}

	private OutboundItemDTO dtoWithPk(Long pk) {
		Stub(OutboundItemDTO) {
			getItem() >> Stub(OutboundItemChange) {
				getPK() >> pk
			}
			getRootItemPK() >> null
		}
	}

	private OutboundItemDTO dtoWithReferenceToRoot(Long pk) {
		Stub(OutboundItemDTO) {
			getItem() >> Stub(OutboundItemChange) {
				getPK() >> pk
			}
			getRootItemPK() >> ROOT_ITEM_PK
		}
	}
}
