EWB_RECOVERY = """
SELECT
    leads_result.leads_result_id            AS RESULT_ID,
    users.users_username                    AS AGENT,
    leads.leads_chcode                      AS CH_CODE,
    leads.leads_chname                      AS CH_NAME,
    leads.leads_acctno                      AS ACCT_NO,
    dv1.dynamic_value_name                  AS PLACEMENT,
    leads_status.leads_status_name          AS STATUS,
    leads_substatus.leads_substatus_name    AS SUB_STATUS,
    leads_result.leads_result_amount        AS AMOUNT,
    leads_result.leads_result_sdate         AS START_DATE,
    leads_result.leads_result_edate         AS END_DATE,
    '' AS OR_NUMBER,
    CASE
        WHEN LEFT(leads_result_comment, 3) = '01_' THEN SUBSTRING(leads_result_comment, 4)
        WHEN LEFT(leads_result_comment, 2) = '0_'  THEN SUBSTRING(leads_result_comment, 3)
        WHEN LEFT(leads_result_comment, 2) = '1_'  THEN SUBSTRING(leads_result_comment, 3)
        ELSE leads_result_comment
    END AS NOTES,
    '' AS NEW_ADDRESS,
    '' AS NEW_CONTACT,
    leads_result.leads_result_barcode_date  AS BARCODE_DATE,
    leads_result.leads_result_source        AS SOURCE,
    leads.leads_endo_date                   AS ENDO_DATE,
    leads.leads_ob                          AS OB
FROM bcrm.leads_result
INNER JOIN bcrm.leads           ON leads_result.leads_result_lead          = leads.leads_id
INNER JOIN bcrm.leads_status    ON leads_result.leads_result_status_id     = leads_status.leads_status_id
INNER JOIN bcrm.leads_substatus ON leads_result.leads_result_substatus_id  = leads_substatus.leads_substatus_id
INNER JOIN bcrm.client          ON leads.leads_client_id                   = client.client_id
INNER JOIN bcrm.users           ON users.users_id                          = leads.leads_users_id
LEFT JOIN  dynamic_value AS dv1 ON dv1.dynamic_value_lead_id               = leads.leads_id
                                AND dv1.dynamic_value_dynamic_id            = 1766
WHERE client.client_name IN ('EWB')
  AND leads_status.leads_status_name <> 'RETURNS'
  AND YEAR(leads_result.leads_result_barcode_date) = YEAR(NOW())
  AND users.users_username != 'POUT'
  AND leads_result.leads_result_hidden = '0'
ORDER BY leads_result.leads_result_ts DESC
"""

EWB_PORTFOLIO = """
SELECT
    leads.leads_acctno      AS ACCT_NO,
    leads.leads_chcode      AS CH_CODE,
    client.client_name      AS BANK,
    leads.leads_chname      AS CH_NAME,
    leads.leads_ob          AS OB,
    dv1.dynamic_value_name  AS PLACEMENT
FROM bcrm.leads
LEFT JOIN bcrm.client
    ON leads.leads_client_id = client.client_id
LEFT JOIN dynamic_value AS dv1
    ON  dv1.dynamic_value_lead_id    = leads.leads_id
    AND dv1.dynamic_value_dynamic_id = 1766
WHERE client.client_name = 'EWB'
  AND leads.leads_users_id <> 659
"""

EWB_PTP_DAILY = """
SELECT
    leads_result.leads_result_id            AS RESULT_ID,
    users.users_username                    AS AGENT,
    leads.leads_chcode                      AS CH_CODE,
    leads.leads_chname                      AS CH_NAME,
    leads.leads_acctno                      AS ACCT_NO,
    leads_status.leads_status_name          AS STATUS,
    leads_substatus.leads_substatus_name    AS SUB_STATUS,
    leads_result.leads_result_amount        AS AMOUNT,
    leads_result.leads_result_sdate         AS START_DATE,
    leads_result.leads_result_edate         AS END_DATE,
    leads_result.leads_result_comment       AS NOTES,
    leads.leads_birthday                    AS BIRTHDATE,
    leads_result.leads_result_barcode_date  AS BARCODE_DATE,
    leads_result.leads_result_source        AS SOURCE,
    leads.leads_endo_date                   AS ENDO_DATE,
    leads.leads_ob                          AS OB,
    dv1.dynamic_value_name                  AS PLACEMENT
FROM bcrm.leads_result
INNER JOIN bcrm.leads           ON leads_result.leads_result_lead          = leads.leads_id
INNER JOIN bcrm.leads_status    ON leads_result.leads_result_status_id     = leads_status.leads_status_id
INNER JOIN bcrm.leads_substatus ON leads_result.leads_result_substatus_id  = leads_substatus.leads_substatus_id
INNER JOIN bcrm.client          ON leads.leads_client_id                   = client.client_id
INNER JOIN bcrm.users           ON users.users_id                          = leads.leads_users_id
LEFT JOIN  dynamic_value AS dv1 ON dv1.dynamic_value_lead_id               = leads.leads_id
                                AND dv1.dynamic_value_dynamic_id            = 1766
LEFT JOIN  dynamic_value AS dv2 ON dv2.dynamic_value_lead_id               = leads.leads_id
                                AND dv2.dynamic_value_dynamic_id            = 243
WHERE client.client_name = 'EWB'
  AND leads_status.leads_status_name IN ('PTP')
  AND MONTH(leads_result.leads_result_sdate)       = MONTH(NOW())
  AND YEAR(leads_result.leads_result_barcode_date) = YEAR(NOW())
  AND leads_result.leads_result_hidden = 0
ORDER BY leads_result.leads_result_barcode_date DESC
"""

EWB_150DPD = """
SELECT
    client.client_name                      AS Banks,
    leads.leads_chcode,
    users_leads.users_username              AS AgentCode,
    leads_status.leads_status_name          AS Status,
    leads_substatus.leads_substatus_name    AS Substatus,
    leads.leads_endo_date,
    leads.leads_new_address,
    users_result.users_username             AS LastTouch,
    leads_result.leads_result_barcode_date  AS LastTouchDate,
    leads.leads_cutoff                      AS PULLOUT_DATE,
    dv1.dynamic_value_name                  AS MAX_OTP,
    leads.leads_lpd,
    leads.leads_lpa,
    leads.leads_ts,
    leads.leads_ob                          AS OB,
    leads_cycle                             AS Cycle,
    leads_result.leads_result_amount        AS Amount,
    leads_result.leads_result_barcode_date  AS PaymentDate
FROM bcrm.leads
LEFT JOIN (
    SELECT leads_result_lead, MAX(leads_result_id) AS leads_result_id
    FROM leads_result
    INNER JOIN leads ON leads.leads_id = leads_result.leads_result_lead
    WHERE leads.leads_client_id = 233
      AND leads_result.leads_result_hidden = '0'
    GROUP BY leads_result_lead
) latest ON latest.leads_result_lead = leads.leads_id
LEFT JOIN leads_result      ON leads_result.leads_result_id        = latest.leads_result_id
LEFT JOIN leads_status      ON leads_status.leads_status_id        = leads_result.leads_result_status_id
LEFT JOIN leads_substatus   ON leads_substatus.leads_substatus_id  = leads_result.leads_result_substatus_id
LEFT JOIN bcrm.client       ON leads.leads_client_id               = client.client_id
LEFT JOIN bcrm.users AS users_result ON leads_result.leads_result_users = users_result.users_id
LEFT JOIN bcrm.users AS users_leads  ON leads.leads_users_id            = users_leads.users_id
LEFT JOIN dynamic_value AS dv1
    ON  dv1.dynamic_value_lead_id    = leads.leads_id
    AND dv1.dynamic_value_dynamic_id = 3513
WHERE leads.leads_users_id <> 659
  AND client.client_name = 'EWB 150 DPD'
ORDER BY leads_result.leads_result_ts DESC
"""

EWB_FIELD_RESULTS = """
SELECT
    leads_result.leads_result_id            AS RESULT_ID,
    users.users_username                    AS AGENT,
    leads.leads_chcode                      AS CH_CODE,
    leads.leads_chname                      AS CH_NAME,
    leads.leads_acctno                      AS ACCT_NO,
    leads_status.leads_status_name          AS STATUS,
    leads_substatus.leads_substatus_name    AS SUB_STATUS,
    leads_result.leads_result_amount        AS AMOUNT,
    leads_result.leads_result_sdate         AS START_DATE,
    leads_result.leads_result_edate         AS END_DATE,
    '' AS OR_NUMBER,
    leads_result.leads_result_comment       AS NOTES,
    '' AS NEW_ADDRESS,
    '' AS NEW_CONTACT,
    leads_result.leads_result_barcode_date  AS BARCODE_DATE,
    leads_result.leads_result_source        AS SOURCE,
    leads.leads_endo_date                   AS ENDO_DATE,
    leads.leads_ob                          AS OB
FROM bcrm.leads_result
INNER JOIN bcrm.leads           ON leads_result.leads_result_lead          = leads.leads_id
INNER JOIN bcrm.leads_status    ON leads_result.leads_result_status_id     = leads_status.leads_status_id
INNER JOIN bcrm.leads_substatus ON leads_result.leads_result_substatus_id  = leads_substatus.leads_substatus_id
INNER JOIN bcrm.client          ON leads.leads_client_id                   = client.client_id
INNER JOIN bcrm.users           ON users.users_id                          = leads.leads_users_id
WHERE client.client_name = 'EWB'
  AND leads_status.leads_status_name IN (
        'BUSINESS VISIT','HOME VISIT',
        'Field Request Dl 1','Field Request Dl 2'
  )
  AND leads_result.leads_result_barcode_date >= '2025-01-01'
  AND leads_result.leads_result_barcode_date <= NOW()
  AND leads_result.leads_result_hidden = 0
ORDER BY leads_result.leads_result_barcode_date DESC
"""

EWB_150DPD_PTP = """
SELECT
    leads_result.leads_result_id            AS RESULT_ID,
    users.users_username                    AS AGENT,
    leads.leads_chcode                      AS CH_CODE,
    leads.leads_chname                      AS CH_NAME,
    leads.leads_acctno                      AS ACCT_NO,
    leads_status.leads_status_name          AS STATUS,
    leads_substatus.leads_substatus_name    AS SUB_STATUS,
    leads_result.leads_result_amount        AS AMOUNT,
    leads_result.leads_result_sdate         AS START_DATE,
    leads_result.leads_result_edate         AS END_DATE,
    leads_result.leads_result_comment       AS NOTES,
    leads.leads_birthday                    AS BIRTHDATE,
    leads_result.leads_result_barcode_date  AS BARCODE_DATE,
    leads_result.leads_result_source        AS SOURCE,
    leads.leads_endo_date                   AS ENDO_DATE,
    leads.leads_ob                          AS OB,
    leads_cycle                             AS Cycle,
    dv1.dynamic_value_name                  AS PLACEMENT
FROM bcrm.leads_result
INNER JOIN bcrm.leads           ON leads_result.leads_result_lead          = leads.leads_id
INNER JOIN bcrm.leads_status    ON leads_result.leads_result_status_id     = leads_status.leads_status_id
INNER JOIN bcrm.leads_substatus ON leads_result.leads_result_substatus_id  = leads_substatus.leads_substatus_id
INNER JOIN bcrm.client          ON leads.leads_client_id                   = client.client_id
INNER JOIN bcrm.users           ON users.users_id                          = leads.leads_users_id
LEFT JOIN  dynamic_value AS dv1 ON dv1.dynamic_value_lead_id               = leads.leads_id
                                AND dv1.dynamic_value_dynamic_id            = 1766
LEFT JOIN  dynamic_value AS dv2 ON dv2.dynamic_value_lead_id               = leads.leads_id
                                AND dv2.dynamic_value_dynamic_id            = 243
WHERE client.client_name = 'EWB 150 DPD'
  AND leads_status.leads_status_name IN ('PTP')
  AND MONTH(leads_result.leads_result_sdate)       = MONTH(NOW())
  AND YEAR(leads_result.leads_result_barcode_date) = YEAR(NOW())
  AND leads_result.leads_result_hidden = 0
ORDER BY leads_result.leads_result_barcode_date DESC
"""

EWB_150DPD_FIELD = """
SELECT
    leads_result.leads_result_id            AS RESULT_ID,
    users.users_username                    AS AGENT,
    leads.leads_chcode                      AS CH_CODE,
    leads.leads_chname                      AS CH_NAME,
    leads.leads_acctno                      AS ACCT_NO,
    leads_status.leads_status_name          AS STATUS,
    leads_substatus.leads_substatus_name    AS SUB_STATUS,
    leads_result.leads_result_amount        AS AMOUNT,
    leads_result.leads_result_sdate         AS START_DATE,
    leads_result.leads_result_edate         AS END_DATE,
    leads_result.leads_result_comment       AS NOTES,
    leads_result.leads_result_barcode_date  AS BARCODE_DATE,
    leads_result.leads_result_source        AS SOURCE,
    leads.leads_endo_date                   AS ENDO_DATE,
    leads.leads_ob                          AS OB,
    leads_cycle                             AS Cycle
FROM bcrm.leads_result
INNER JOIN bcrm.leads           ON leads_result.leads_result_lead          = leads.leads_id
INNER JOIN bcrm.leads_status    ON leads_result.leads_result_status_id     = leads_status.leads_status_id
INNER JOIN bcrm.leads_substatus ON leads_result.leads_result_substatus_id  = leads_substatus.leads_substatus_id
INNER JOIN bcrm.client          ON leads.leads_client_id                   = client.client_id
INNER JOIN bcrm.users           ON users.users_id                          = leads.leads_users_id
WHERE client.client_name = 'EWB 150 DPD'
  AND leads_status.leads_status_name IN (
        'BUSINESS VISIT','HOME VISIT',
        'Field Request Dl 1','Field Request Dl 2'
  )
  AND leads_result.leads_result_barcode_date >= '2025-01-01'
  AND leads_result.leads_result_barcode_date <= NOW()
  AND leads_result.leads_result_hidden = 0
ORDER BY leads_result.leads_result_barcode_date DESC
"""

def ewb_150dpd_efforts_query(month: int, year: int) -> str:
    """All efforts for EWB 150 DPD for a given month/year — used for Worked Accounts + Total Efforts."""
    return f"""
SELECT
    leads_result.leads_result_id            AS RESULT_ID,
    users.users_username                    AS AGENT,
    leads.leads_chcode                      AS CH_CODE,
    leads.leads_chname                      AS CH_NAME,
    leads.leads_acctno                      AS ACCT_NO,
    leads_status.leads_status_name          AS STATUS,
    leads_substatus.leads_substatus_name    AS SUB_STATUS,
    leads_result.leads_result_amount        AS AMOUNT,
    leads_result.leads_result_sdate         AS START_DATE,
    leads_result.leads_result_edate         AS END_DATE,
    '' AS OR_NUMBER,
    CASE
        WHEN LEFT(leads_result_comment, 3) = '01_' THEN SUBSTRING(leads_result_comment, 4)
        WHEN LEFT(leads_result_comment, 2) = '0_'  THEN SUBSTRING(leads_result_comment, 3)
        WHEN LEFT(leads_result_comment, 2) = '1_'  THEN SUBSTRING(leads_result_comment, 3)
        ELSE leads_result_comment
    END AS NOTES,
    '' AS NEW_ADDRESS,
    '' AS NEW_CONTACT,
    leads_result.leads_result_barcode_date  AS BARCODE_DATE,
    leads_result_source                     AS SOURCE,
    leads.leads_endo_date                   AS ENDO_DATE,
    leads.leads_ob                          AS OB,
    leads.leads_cycle                       AS Cycle
FROM bcrm.leads_result
INNER JOIN bcrm.leads
    ON leads_result.leads_result_lead = leads.leads_id
INNER JOIN bcrm.leads_status
    ON leads_result.leads_result_status_id = leads_status.leads_status_id
INNER JOIN bcrm.leads_substatus
    ON leads_result.leads_result_substatus_id = leads_substatus.leads_substatus_id
INNER JOIN bcrm.client
    ON leads.leads_client_id = client.client_id
INNER JOIN bcrm.users
    ON users.users_id = leads.leads_users_id
WHERE client.client_name IN ('EWB 150 DPD')
  AND leads_status.leads_status_name <> 'RETURNS'
  AND MONTH(leads_result.leads_result_barcode_date) = {month}
  AND YEAR(leads_result.leads_result_barcode_date)  = {year}
  AND users.users_username != 'POUT'
  AND leads_result.leads_result_hidden = '0'
ORDER BY leads_result.leads_result_ts DESC
"""
