WITH base AS (SELECT DISTINCT itemDescription, ' - ' AS sep, 3 AS len_sep
                                 FROM            (SELECT        itemDescription
                                                           FROM            Book.APIPayments.Payments WITH (nolock)
                                                           UNION ALL
                                                           SELECT        itemDescription
                                                           FROM            BookComercial.APIPayments.Payments WITH (nolock)) AS b), t1 AS
    (SELECT        itemDescription, REPLACE(itemDescription, '.', '') AS itemDescription2, sep, len_sep, CASE WHEN charindex(sep, REPLACE(itemDescription, '.', '')) <> 0 THEN charindex(sep, REPLACE(itemDescription, '.', '')) 
                                END AS indice_primeiro_sep
      FROM            base), t1_2 AS
    (SELECT        itemDescription, itemDescription2, sep, len_sep, indice_primeiro_sep, SUBSTRING(itemDescription2, indice_primeiro_sep + len_sep, LEN(itemDescription2)) AS texto_apos_primeiro_sep, SUBSTRING(itemDescription2, 0, 
                                indice_primeiro_sep) AS texto_antes_primeiro_sep
      FROM            t1), t2 AS
    (SELECT        itemDescription, itemDescription2, sep, len_sep, indice_primeiro_sep, texto_apos_primeiro_sep, texto_antes_primeiro_sep, CASE WHEN charindex(sep, texto_apos_primeiro_sep) <> 0 THEN charindex(sep, 
                                texto_apos_primeiro_sep) + len(texto_antes_primeiro_sep) + len_sep END AS indice_segundo_sep
      FROM            t1_2), t2_2 AS
    (SELECT        itemDescription, itemDescription2, sep, len_sep, indice_primeiro_sep, texto_apos_primeiro_sep, texto_antes_primeiro_sep, indice_segundo_sep, SUBSTRING(itemDescription2, indice_segundo_sep + len_sep, 
                                LEN(itemDescription2)) AS texto_apos_segundo_sep, SUBSTRING(itemDescription2, 0, indice_segundo_sep) AS texto_antes_segundo_sep
      FROM            t2), t3 AS
    (SELECT        itemDescription, itemDescription2, sep, len_sep, indice_primeiro_sep, texto_apos_primeiro_sep, texto_antes_primeiro_sep, indice_segundo_sep, texto_apos_segundo_sep, texto_antes_segundo_sep, 
                                SUBSTRING(itemDescription2, indice_primeiro_sep + len_sep, (LEN(itemDescription2) - LEN(texto_antes_primeiro_sep) - LEN(texto_apos_segundo_sep)) - 2 * len_sep) AS texto_entre_seps
      FROM            t2_2), t4 AS
    (SELECT        itemDescription, itemDescription2, sep, len_sep, indice_primeiro_sep, texto_apos_primeiro_sep, texto_antes_primeiro_sep, indice_segundo_sep, texto_apos_segundo_sep, texto_antes_segundo_sep, texto_entre_seps, 
                                CASE WHEN indice_segundo_sep IS NOT NULL THEN texto_antes_primeiro_sep END AS codigo, CASE WHEN indice_segundo_sep IS NOT NULL THEN texto_entre_seps END AS entrega, FORMAT(TRY_CONVERT(DATE, 
                                '01-' + CASE WHEN itemDescription LIKE '%JAN%' THEN '01' WHEN itemDescription LIKE '%FEV%' THEN '02' WHEN itemDescription LIKE '%MAR%' THEN '03' WHEN itemDescription LIKE '%ABR%' THEN '04' WHEN itemDescription
                                 LIKE '%MAI%' THEN '05' WHEN itemDescription LIKE '%JUN%' THEN '06' WHEN itemDescription LIKE '%JUL%' THEN '07' WHEN itemDescription LIKE '%AGO%' THEN '08' WHEN itemDescription LIKE '%SET%' THEN '09' WHEN
                                 itemDescription LIKE '%OUT%' THEN '10' WHEN itemDescription LIKE '%NOV%' THEN '11' WHEN itemDescription LIKE '%DEZ%' THEN '12' ELSE NULL END + '-' + RIGHT(itemDescription, 4), 105), 'yyyy-MM-dd') 
                                AS datafornecimento
      FROM            t3), final AS
    (SELECT        'Payments' AS registry_type, t4.codigo, t4.entrega, t4.datafornecimento, 'Safira' AS Thunders, e.orderTradeTypeId, e.orderTradeType, e.orderId, e.orderCode, e.clientOrderNumber, e.invoiceId, e.invoiceCode, 
                                e.invoiceSequence, e.orderCodeCcee, e.partyId, e.partyCnpj, e.partyName, e.partyAliasName, e.counterPartyId, e.counterPartyCpfCnpj, e.counterpartyName, e.counterPartyTypeId, e.counterPartyAliasName, 
                                e.counterPartyState, e.invoiceLastStatusDate, e.presentationDate, e.dueDate, e.invoiceStatusId, e.invoiceStatusDescription, e.invoiceNumber, e.invoiceSerie, e.netValue, e.totalValue, e.hasMultipleItens, e.itemQuantity, 
                                e.itemTotalValue, e.itemUnitPrice, e.itemUnitPriceWithIcms, e.invoiceIcmsValue, e.invoiceDiscountValue, e.invoiceItemIcmsAliquot, e.invoiceCfop, e.itemDescription, e.paymentID, e.previsionDate, e.effectiveDate, 
                                e.paymentStatusId, e.paymentStatus, e.previsionValue, e.discountValue, e.penalityValue, e.delayedValue, e.remainingValue, e.effectiveValue, e.discountedNetInstallment, e.finalValue, e.orderItemTypeId, e.orderItemType, 
                                e.ordemItemClassifications, e.consolidationFirst, e.consolidationSecond, e.isCreatedByUser, e.hasIntegrationError, e.approvedDate, e.invoiceExtendedPropertyValues, e.companyExtendedPropertyValues, 
                                e.reimbursementValue, e.DataInsertTable
      FROM            Book.APIPayments.Payments AS e WITH (nolock) LEFT OUTER JOIN
                                t4 ON e.itemDescription = t4.itemDescription
      UNION ALL
      SELECT        'Payments' AS registry_type, t4.codigo, t4.entrega, t4.datafornecimento, 'Comercial' AS Thunders, e.orderTradeTypeId, e.orderTradeType, e.orderId, e.orderCode, e.clientOrderNumber, e.invoiceId, e.invoiceCode, 
                               e.invoiceSequence, e.orderCodeCcee, e.partyId, e.partyCnpj, e.partyName, e.partyAliasName, e.counterPartyId, e.counterPartyCpfCnpj, e.counterpartyName, e.counterPartyTypeId, e.counterPartyAliasName, 
                               e.counterPartyState, e.invoiceLastStatusDate, e.presentationDate, e.dueDate, e.invoiceStatusId, e.invoiceStatusDescription, e.invoiceNumber, e.invoiceSerie, e.netValue, e.totalValue, e.hasMultipleItens, e.itemQuantity, 
                               e.itemTotalValue, e.itemUnitPrice, e.itemUnitPriceWithIcms, e.invoiceIcmsValue, e.invoiceDiscountValue, e.invoiceItemIcmsAliquot, e.invoiceCfop, e.itemDescription, e.paymentID, e.previsionDate, e.effectiveDate, 
                               e.paymentStatusId, e.paymentStatus, e.previsionValue, e.discountValue, e.penalityValue, e.delayedValue, e.remainingValue, e.effectiveValue, e.discountedNetInstallment, e.finalValue, e.orderItemTypeId, e.orderItemType, 
                               e.ordemItemClassifications, e.consolidationFirst, e.consolidationSecond, e.isCreatedByUser, e.hasIntegrationError, e.approvedDate, e.invoiceExtendedPropertyValues, e.companyExtendedPropertyValues, 
                               e.reimbursementValue, e.DataInsertTable
      FROM            BookComercial.APIPayments.Payments AS e WITH (nolock) LEFT OUTER JOIN
                               t4 ON e.itemDescription = t4.itemDescription)
    SELECT        registry_type, Thunders, codigo + '-' + Thunders + '-' + entrega AS distinctCode2, itemDescription AS item_description, codigo, entrega, datafornecimento, orderTradeType AS order_trade_type, partyCnpj AS party_cnpj, 
                              partyName AS party_name, counterPartyCpfCnpj AS counterparty_cpf_cnpj, counterpartyName AS counterparty_name, counterPartyState AS counterparty_state, CASE WHEN CAST(presentationDate AS date) 
                              <> '0001-01-01' THEN CAST(presentationDate AS date) END AS presentation_date, CASE WHEN CAST(dueDate AS date) <> '0001-01-01' THEN CAST(dueDate AS date) END AS due_date, 
                              invoiceStatusDescription AS invoice_status_description, invoiceNumber AS invoice_number, orderCode AS order_code, invoiceCode AS invoice_code, orderCodeCcee AS order_code_ccee, 
                              clientOrderNumber AS client_order_number, netValue AS net_value, totalValue AS total_value, itemQuantity AS item_quantity, itemTotalValue AS item_total_value, itemUnitPrice AS item_unit_price, 
                              itemUnitPriceWithIcms AS item_unit_price_with_icms, invoiceIcmsValue AS invoice_icms_value, invoiceDiscountValue AS invoice_discount_value, invoiceItemIcmsAliquot AS invoice_item_icms_aliquot, 
                              invoiceCfop AS invoice_cfop, CASE WHEN CAST(previsionDate AS date) <> '0001-01-01' THEN CAST(previsionDate AS date) END AS prevision_date, CASE WHEN CAST(effectiveDate AS date) 
                              <> '0001-01-01' THEN CAST(effectiveDate AS date) END AS effective_date, paymentStatus AS payment_status, previsionValue AS prevision_value, discountValue AS discount_value, penalityValue AS penality_value, 
                              delayedValue AS delayed_value, remainingValue AS remaining_value, effectiveValue AS effective_value, discountedNetInstallment AS discounted_net_installment, previsionValue * (1 - invoiceItemIcmsAliquot / 100) 
                              AS discounted_net_installment_calculated, previsionValue * (invoiceItemIcmsAliquot / 100) AS valor_icms_liquido_parcela, orderItemType AS order_item_type, CASE WHEN CAST(approvedDate AS date) 
                              <> '0001-01-01' THEN CAST(approvedDate AS date) END AS approved_date
     FROM            final