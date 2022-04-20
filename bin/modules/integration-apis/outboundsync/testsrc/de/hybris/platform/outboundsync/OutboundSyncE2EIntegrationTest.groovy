/*
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */

package de.hybris.platform.outboundsync

import de.hybris.bootstrap.annotations.IntegrationTest
import de.hybris.platform.apiregistryservices.model.ConsumedDestinationModel
import de.hybris.platform.apiregistryservices.model.DestinationTargetModel
import de.hybris.platform.apiregistryservices.model.EndpointModel
import de.hybris.platform.core.model.product.ProductModel
import de.hybris.platform.core.model.user.CustomerModel
import de.hybris.platform.core.model.user.EmployeeModel
import de.hybris.platform.cronjob.enums.CronJobResult
import de.hybris.platform.cronjob.model.CronJobModel
import de.hybris.platform.integrationservices.model.IntegrationObjectModel
import de.hybris.platform.integrationservices.util.IntegrationTestUtil
import de.hybris.platform.outboundservices.facade.OutboundServiceFacade
import de.hybris.platform.outboundsync.activator.impl.DefaultOutboundSyncService
import de.hybris.platform.outboundsync.model.OutboundChannelConfigurationModel
import de.hybris.platform.outboundsync.util.OutboundSyncTestUtil
import de.hybris.platform.servicelayer.ServicelayerSpockSpecification
import de.hybris.platform.servicelayer.cronjob.CronJobService
import org.junit.Rule
import org.junit.Test

import javax.annotation.Resource

import static de.hybris.platform.integrationservices.util.IntegrationTestUtil.condition
import static de.hybris.platform.outboundsync.util.OutboundSyncTestUtil.importProductWithCode
import static de.hybris.platform.outboundservices.ConsumedDestinationBuilder.consumedDestinationBuilder

@IntegrationTest
/*
 Since outbound sync is multi-threaded, using ServiceLayerTransactionalSpockSpecification
 causes the sync method not to find the item. With the sync method running in a different thread,
 it is outside the transaction of this test thread.
 */
class OutboundSyncE2EIntegrationTest extends ServicelayerSpockSpecification {
    private static final def PRODUCT_IO = 'OutboundProduct'
    private static final def CUSTOMER_IO = 'OutboundCustomer'

    private static final def CATALOG_VERSION = IntegrationTestUtil.importCatalogVersion('Test', 'Default', true)
    private static final ConsumedDestinationModel DESTINATION = consumedDestinationBuilder().withId('outbound').build()

    @Resource
    private CronJobService cronJobService
    @Resource(name = 'outboundSyncService')
    private DefaultOutboundSyncService outboundSyncService
    @Resource(name = 'outboundServiceFacade')
    private OutboundServiceFacade outboundServiceFacade

    @Rule
    TestOutboundFacade outboundDestination = TestOutboundFacade.respondWithCreated()
    @Rule
    TestItemChangeDetector changeDetector = new TestItemChangeDetector()

    CronJobModel cronJob

    def setup() {
        outboundSyncService.setOutboundServiceFacade(outboundDestination)
        importCsv '/impex/essentialdata-outboundsync.impex', 'UTF-8'
        cronJob = OutboundSyncTestUtil.outboundCronJob()
    }

    def cleanup() {
        IntegrationTestUtil.removeAll OutboundChannelConfigurationModel
        IntegrationTestUtil.removeAll IntegrationObjectModel
        IntegrationTestUtil.removeAll ProductModel
        IntegrationTestUtil.remove cronJob

        outboundSyncService.setOutboundServiceFacade(outboundServiceFacade)
    }

    def cleanupSpec() {
        // endpoint deletes consumed destinations through cascade
        IntegrationTestUtil.removeAll EndpointModel
        IntegrationTestUtil.removeAll DestinationTargetModel
    }

    @Test
    def "no updates sent when no updates to Products"() {
        given: "an IntegrationObject for Product"
        IntegrationTestUtil.importImpEx(
                'INSERT_UPDATE IntegrationObject; code[unique = true]',
                "                               ; $PRODUCT_IO",
                'INSERT_UPDATE IntegrationObjectItem; integrationObject(code)[unique = true]; code[unique = true]; type(code) ; root[default = false]',
                "                                   ; $PRODUCT_IO                           ; Product            ; Product    ; true",
                "                                   ; $PRODUCT_IO                           ; Catalog            ; Catalog",
                "                                   ; $PRODUCT_IO                           ; CatalogVersion     ; CatalogVersion",
                '$integrationItem = integrationObjectItem(integrationObject(code), code)[unique = true]',
                '$attributeName = attributeName[unique = true]',
                '$attributeDescriptor = attributeDescriptor(enclosingType(code), qualifier)',
                'INSERT_UPDATE IntegrationObjectItemAttribute; $integrationItem           ; $attributeName ; $attributeDescriptor   ; returnIntegrationObjectItem(integrationObject(code), code)',
                "                                            ; $PRODUCT_IO:Catalog        ; id             ; Catalog:id             ;",
                "                                            ; $PRODUCT_IO:CatalogVersion ; catalog        ; CatalogVersion:catalog ; $PRODUCT_IO:Catalog",
                "                                            ; $PRODUCT_IO:CatalogVersion ; version        ; CatalogVersion:version ;",
                "                                            ; $PRODUCT_IO:Product        ; code           ; Product:code           ;",
                "                                            ; $PRODUCT_IO:Product        ; catalogVersion ; Product:catalogVersion ; $PRODUCT_IO:CatalogVersion")
        and: "Outbound sync channel listens for Product changes"
        IntegrationTestUtil.importImpEx(
                'INSERT_UPDATE OutboundChannelConfiguration	; code[unique = true] ; integrationObject(code) ; destination',
                "											; outboundProducts    ; $PRODUCT_IO             ; $DESTINATION.pk")
                changeDetector.createChangeStream 'outboundProducts', 'Product'

        when: "no changes were made to a Product"
        cronJobService.performCronJob(cronJob, true)

        then: "the job sent no updates"
        condition().eventually {
            assert cronJob.result == CronJobResult.SUCCESS
            assert outboundDestination.invocations() == 0
        }
    }

    @Test
    def "no updates sent when root item cannot be derived from the changed item model"() {
        given: 'IO for Product has a nested entity for Unit that does not refer batck to Product'
        IntegrationTestUtil.importImpEx(
                'INSERT_UPDATE IntegrationObject; code[unique = true]',
                "                               ; $PRODUCT_IO",
                'INSERT_UPDATE IntegrationObjectItem; integrationObject(code)[unique = true]; code[unique = true]; type(code) ; root[default = false]',
                "                                   ; $PRODUCT_IO                           ; Product            ; Product    ; true",
                "                                   ; $PRODUCT_IO                           ; Unit               ; Unit",
                "                                   ; $PRODUCT_IO                           ; Catalog            ; Catalog",
                "                                   ; $PRODUCT_IO                           ; CatalogVersion     ; CatalogVersion",
                '$integrationItem = integrationObjectItem(integrationObject(code), code)[unique = true]',
                '$attributeName = attributeName[unique = true]',
                '$attributeDescriptor = attributeDescriptor(enclosingType(code), qualifier)',
                'INSERT_UPDATE IntegrationObjectItemAttribute; $integrationItem           ; $attributeName ; $attributeDescriptor   ; returnIntegrationObjectItem(integrationObject(code), code)',
                "                                            ; $PRODUCT_IO:Catalog        ; id             ; Catalog:id             ;",
                "                                            ; $PRODUCT_IO:CatalogVersion ; catalog        ; CatalogVersion:catalog ; $PRODUCT_IO:Catalog",
                "                                            ; $PRODUCT_IO:CatalogVersion ; version        ; CatalogVersion:version ;",
                "                                            ; $PRODUCT_IO:Unit           ; code           ; Unit:code              ;",
                "                                            ; $PRODUCT_IO:Product        ; code           ; Product:code           ;",
                "                                            ; $PRODUCT_IO:Product        ; unit           ; Product:unit           ; $PRODUCT_IO:Unit",
                "                                            ; $PRODUCT_IO:Product        ; catalogVersion ; Product:catalogVersion ; $PRODUCT_IO:CatalogVersion")
        and: 'product with unit is created'
        IntegrationTestUtil.importImpEx(
                'INSERT_UPDATE Unit ; code[unique = true]; unitType',
                '                   ; box                ; package',
                'INSERT_UPDATE Product ; code[unique = true] ; unit(code) ; catalogVersion',
                "                      ; root                ; box        ; $CATALOG_VERSION.pk")
        and: 'Outbound sync channel listens for Unit and Product changes after they created'
        IntegrationTestUtil.importImpEx(
                'INSERT_UPDATE OutboundChannelConfiguration	; code[unique = true] ; integrationObject(code) ; destination',
                "											; outboundProducts    ; $PRODUCT_IO             ; $DESTINATION.pk")
        changeDetector.createChangeStream 'outboundProducts', 'Product'
        changeDetector.createChangeStream 'outboundProducts', 'Unit'
        and: 'the unit is changed'
        IntegrationTestUtil.importImpEx(
                'UPDATE Unit ; code[unique = true] ; name[lang = en]',
                "            ; box                 ; Box")

        when:
        cronJobService.performCronJob(cronJob, true)

        then:
        condition().eventually {
            assert cronJob.result == CronJobResult.SUCCESS
            assert outboundDestination.invocations() == 0
            assert changeDetector.hasAllChangesConsumed()
        }
    }

    @Test
    def "no updates sent when changed child item is not present in the IntegrationObject model"() {
        given: "integration object for Customer is defined without Address"
        IntegrationTestUtil.importImpEx(
                'INSERT_UPDATE IntegrationObject; code[unique = true]',
                "                               ; $CUSTOMER_IO",
                'INSERT_UPDATE IntegrationObjectItem; integrationObject(code)[unique = true]; code[unique = true]; type(code) ; root',
                "                                   ; $CUSTOMER_IO                          ; Customer           ; Customer   ; true",
                '$integrationItem = integrationObjectItem(integrationObject(code), code)[unique = true]',
                '$attributeName = attributeName[unique = true]',
                'INSERT_UPDATE IntegrationObjectItemAttribute; $integrationItem      ; $attributeName ; attributeDescriptor(enclosingType(code), qualifier)',
                "                                            ; $CUSTOMER_IO:Customer ; uid            ; Customer:uid")
        and: "a Customer is created with Address"
        IntegrationTestUtil.importImpEx(
                'INSERT_UPDATE Customer; uid[unique = true]; defaultPaymentAddress( &addrID )',
                '                      ; user123           ; theAddress',
                'INSERT_UPDATE Address; &addrID   ; email[unique = true] ; owner(Customer.uid); company',
                '                     ; theAddress; user123@some.net     ; user123            ; hybris')
        and: "Outbound sync channel listens for Customer and Address changes after they were created"
        IntegrationTestUtil.importImpEx(
                'INSERT_UPDATE OutboundChannelConfiguration	; code[unique = true] ; integrationObject(code) ; destination',
                "											; outboundCustomers   ; $CUSTOMER_IO            ; $DESTINATION.pk")
        changeDetector.createChangeStream 'outboundCustomers', 'Customer'
        changeDetector.createChangeStream 'outboundCustomers', 'Address'
        and: "the Address has changed"
        IntegrationTestUtil.importImpEx(
                'UPDATE Address; email[unique = true] ; company',
                '              ; user123@some.net     ; SAP')

        when: "the sync job runs"
        cronJobService.performCronJob(cronJob, true)

        then: "no changes sent"
        condition().eventually {
            assert cronJob.result == CronJobResult.SUCCESS
            // No changes sent to the destination(s)
            assert outboundDestination.invocations() == 0
            // Address change is consumed
            assert changeDetector.hasAllChangesConsumed()
        }

        cleanup:
        IntegrationTestUtil.findAny(CustomerModel, { it.uid == 'user123' }).ifPresent { IntegrationTestUtil.remove it }
    }

    @Test
    def "no updates sent when root item derived from the changed child is not present in the IntegrationObject model"() {
        given: "integration object for Customer and Address"
        IntegrationTestUtil.importImpEx(
                'INSERT_UPDATE IntegrationObject; code[unique = true]',
                "                               ; $CUSTOMER_IO",
                'INSERT_UPDATE IntegrationObjectItem; integrationObject(code)[unique = true]; code[unique = true]; type(code) ; root',
                "                                   ; $CUSTOMER_IO                          ; Customer           ; Customer   ; true",
                "                                   ; $CUSTOMER_IO                          ; Address            ; Address    ; false",
                '$integrationItem = integrationObjectItem(integrationObject(code), code)[unique = true]',
                '$attributeName = attributeName[unique = true]',
                '$attributeDescriptor = attributeDescriptor(enclosingType(code), qualifier)',
                '$returnItem = returnIntegrationObjectItem(integrationObject(code), code)',
                'INSERT_UPDATE IntegrationObjectItemAttribute; $integrationItem      ; $attributeName ; $attributeDescriptor           ; $returnItem          ; unique',
                "                                            ; $CUSTOMER_IO:Customer ; uid            ; Customer:uid",
                "                                            ; $CUSTOMER_IO:Customer ; paymentAddress ; Customer:defaultPaymentAddress ; $CUSTOMER_IO:Address",
                "                                            ; $CUSTOMER_IO:Address  ; email          ; Address:email                  ;                      ; true")
        and: "Outbound sync channel listens for Customer and Address changes"
        IntegrationTestUtil.importImpEx(
                'INSERT_UPDATE OutboundChannelConfiguration	; code[unique = true] ; integrationObject(code) ; destination',
                "											; outboundCustomers   ; $CUSTOMER_IO            ; $DESTINATION.pk")
        changeDetector.createChangeStream 'outboundCustomers', 'Customer'
        changeDetector.createChangeStream 'outboundCustomers', 'Address'
        and: "an Employee (not a Customer) is created with Address"
        IntegrationTestUtil.importImpEx(
                'INSERT_UPDATE Employee; uid[unique = true]; defaultPaymentAddress( &addrID )',
                '                      ; user123           ; theAddress',
                'INSERT_UPDATE Address; &addrID   ; owner(Employee.uid); email[unique = true]',
                '                     ; theAddress; user123            ; a.b@some.net')

        when:
        cronJobService.performCronJob(cronJob, true)

        then: "the job does not send updates because Employee is not present in the IntegrationObject model"
        condition().eventually {
            assert cronJob.result == CronJobResult.SUCCESS
            // No changes sent to the destination(s)
            assert outboundDestination.invocations() == 0
            // Address change is consumed
            assert changeDetector.hasAllChangesConsumed()
        }

        cleanup:
        IntegrationTestUtil.findAny(EmployeeModel, { it.uid == 'user123' }).ifPresent { IntegrationTestUtil.remove it }
    }

    @Test
    def "sends update only for Product deltas when Product and its child has changed"() {
        given: "IntegrationObject is defined for Product root item"
        IntegrationTestUtil.importImpEx(
                'INSERT_UPDATE IntegrationObject; code[unique = true]',
                "                               ; $PRODUCT_IO",
                'INSERT_UPDATE IntegrationObjectItem; integrationObject(code)[unique = true]; code[unique = true]; type(code) ; root[default = false]',
                "                                   ; $PRODUCT_IO                           ; Product            ; Product    ; true",
                "                                   ; $PRODUCT_IO                           ; Catalog            ; Catalog",
                "                                   ; $PRODUCT_IO                           ; CatalogVersion     ; CatalogVersion",
                '$integrationItem = integrationObjectItem(integrationObject(code), code)[unique = true]',
                '$attributeName = attributeName[unique = true]',
                '$attributeDescriptor = attributeDescriptor(enclosingType(code), qualifier)',
                'INSERT_UPDATE IntegrationObjectItemAttribute; $integrationItem           ; $attributeName ; $attributeDescriptor   ; returnIntegrationObjectItem(integrationObject(code), code)',
                "                                            ; $PRODUCT_IO:Catalog        ; id             ; Catalog:id             ;",
                "                                            ; $PRODUCT_IO:CatalogVersion ; catalog        ; CatalogVersion:catalog ; $PRODUCT_IO:Catalog",
                "                                            ; $PRODUCT_IO:CatalogVersion ; version        ; CatalogVersion:version ;",
                "                                            ; $PRODUCT_IO:Product        ; code           ; Product:code           ;",
                "                                            ; $PRODUCT_IO:Product        ; catalogVersion ; Product:catalogVersion ; $PRODUCT_IO:CatalogVersion")
        and: 'Outbound sync channel listens for Product and CatalogVersion changes'
        IntegrationTestUtil.importImpEx(
                'INSERT_UPDATE OutboundChannelConfiguration	; code[unique = true] ; integrationObject(code) ; destination',
                "											; outboundProducts    ; $PRODUCT_IO             ; $DESTINATION.pk")
        changeDetector.createChangeStream 'outboundProducts', 'Product'
        changeDetector.createChangeStream 'outboundProducts', 'CatalogVersion'
        and: "Product and the CatalogVersion have changed"
        importProductWithCode("prod1")

        when:
        cronJobService.performCronJob(cronJob, true)

        then:
        condition().eventually {
            assert cronJob.result == CronJobResult.SUCCESS
            assert outboundDestination.invocations() == 1
            assert outboundDestination.itemsFromInvocationsTo(DESTINATION, PRODUCT_IO)
                    .collect({ it.itemtype }) == ['Product']
            assert changeDetector.hasAllChangesConsumed()
        }
    }

    @Test
    def "sends update only for root item(s) when their child item(s) changed"() {
        given: "integration object for Product is defined with Keyword child"
        IntegrationTestUtil.importImpEx(
                'INSERT_UPDATE IntegrationObject; code[unique = true]',
                "                               ; $PRODUCT_IO",
                'INSERT_UPDATE IntegrationObjectItem; integrationObject(code)[unique = true]; code[unique = true]; type(code) ; root[default = false]',
                "                                   ; $PRODUCT_IO                           ; Product            ; Product    ; true",
                "                                   ; $PRODUCT_IO                           ; Keyword            ; Keyword",
                "                                   ; $PRODUCT_IO                           ; Catalog            ; Catalog",
                "                                   ; $PRODUCT_IO                           ; CatalogVersion     ; CatalogVersion",
                '$integrationItem = integrationObjectItem(integrationObject(code), code)[unique = true]',
                '$attributeName = attributeName[unique = true]',
                '$attributeDescriptor = attributeDescriptor(enclosingType(code), qualifier)',
                'INSERT_UPDATE IntegrationObjectItemAttribute; $integrationItem           ; $attributeName ; $attributeDescriptor   ; returnIntegrationObjectItem(integrationObject(code), code)',
                "                                            ; $PRODUCT_IO:Catalog        ; id             ; Catalog:id             ;",
                "                                            ; $PRODUCT_IO:CatalogVersion ; catalog        ; CatalogVersion:catalog ; $PRODUCT_IO:Catalog",
                "                                            ; $PRODUCT_IO:CatalogVersion ; version        ; CatalogVersion:version ;",
                "                                            ; $PRODUCT_IO:Keyword        ; keyword        ; Keyword:keyword        ;",
                "                                            ; $PRODUCT_IO:Keyword        ; products       ; Keyword:products       ; $PRODUCT_IO:Product",
                "                                            ; $PRODUCT_IO:Product        ; code           ; Product:code           ;",
                "                                            ; $PRODUCT_IO:Product        ; catalogVersion ; Product:catalogVersion ; $PRODUCT_IO:CatalogVersion",
                "                                            ; $PRODUCT_IO:Product        ; keywords       ; Product:keywords       ; $PRODUCT_IO:Keyword")
        and: "two products with the same keyword created"
        IntegrationTestUtil.importImpEx(
                "INSERT_UPDATE Product ; code[unique = true] ; catalogVersion",
                "                      ; prod1               ; $CATALOG_VERSION.pk",
                "                      ; prod2               ; $CATALOG_VERSION.pk",
                "INSERT_UPDATE Keyword ; keyword[unique = true] ; catalogVersion      ; language(isocode); products(code)",
                "                      ; test                   ; $CATALOG_VERSION.pk ; en               ; prod1")
        and: "Outbound channel listens for Product and Keyword changes after they were created"
        IntegrationTestUtil.importImpEx(
                'INSERT_UPDATE OutboundChannelConfiguration	; code[unique = true] ; integrationObject(code) ; destination',
                "											; outboundProducts    ; $PRODUCT_IO             ; $DESTINATION.pk")
        changeDetector.createChangeStream 'outboundProducts', 'Product'
        changeDetector.createChangeStream 'outboundProducts', 'Keyword'
        and: "the Keyword has changed by adding second Product"
        IntegrationTestUtil.importImpEx(
                "UPDATE Keyword ; keyword[unique = true] ; products(code)",
                "               ; test                   ; prod2")

        when:
        cronJobService.performCronJob(cronJob, true)

        then: "update sent for both Products referred from the changed Keyword"
        condition().eventually {
            // both products were notified
            assert outboundDestination.itemsFromInvocationsTo(DESTINATION, PRODUCT_IO)
                    .collect({ it.code }).containsAll(['prod1', 'prod2'])
            // the Keyword change is consumed
            assert changeDetector.hasAllChangesConsumed()
        }
    }

    @Test
    def "sends updates for subtypes of the integration object root item type"() {
        given: "integration object for User root type"
        IntegrationTestUtil.importImpEx(
                'INSERT_UPDATE IntegrationObject; code[unique = true]',
                "                               ; $CUSTOMER_IO",
                'INSERT_UPDATE IntegrationObjectItem; integrationObject(code)[unique = true]; code[unique = true]; type(code) ; root',
                "                                   ; $CUSTOMER_IO                          ; User               ; User   ; true",
                '$integrationItem = integrationObjectItem(integrationObject(code), code)[unique = true]',
                '$attributeName = attributeName[unique = true]',
                '$attributeDescriptor = attributeDescriptor(enclosingType(code), qualifier)',
                'INSERT_UPDATE IntegrationObjectItemAttribute; $integrationItem  ; $attributeName ; $attributeDescriptor',
                "                                            ; $CUSTOMER_IO:User ; uid            ; Customer:uid")
        and: "Outbound sync channel listens for User changes"
        IntegrationTestUtil.importImpEx(
                'INSERT_UPDATE OutboundChannelConfiguration	; code[unique = true]; integrationObject(code); destination',
                "											; outboundUsers      ; $CUSTOMER_IO           ; $DESTINATION.pk")
        changeDetector.createChangeStream 'outboundUsers', 'Customer'
        and: 'a subclass of User (Customer) is created'
        IntegrationTestUtil.importImpEx(
                'INSERT_UPDATE Customer; uid[unique = true]',
                '                      ; subuser           ')

        when: 'the outbound sync is performed'
        cronJobService.performCronJob(cronJob, true)

        then: 'the item that is a subtype of the root item type is sent out'
        condition().eventually {
            assert cronJob.result == CronJobResult.SUCCESS
            assert outboundDestination.invocations() == 1
            assert outboundDestination.itemsFromInvocationsTo(DESTINATION, CUSTOMER_IO)
                    .collect({ it.itemtype }) == ['Customer']
            assert changeDetector.hasAllChangesConsumed()
        }
    }
}
