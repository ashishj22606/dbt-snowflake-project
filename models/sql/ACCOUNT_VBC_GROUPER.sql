{{
    config(
        materialized='table',
        schema='final_tables',
        alias='account_vbc_grouper',
        tags=['core', 'vbc', 'grouper'],
        unique_key='account_vbc_grouper_sk'
    )
}}

-- Build a single view of ACCOUNT_KEY from both entity and deal terms sources
WITH reparent AS (
    SELECT
        level3_uuid,
        level2_id,
        level1_name
    FROM {{ ref('dbt_stage_merge_reparent_uuid') }}
    WHERE level3_uuid IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY level3_uuid 
        ORDER BY level2_id, level1_name
    ) = 1
),

entity_src AS (
    SELECT
        r.level3_uuid AS account_key,
        r.level2_id,
        r.level1_name,
        a.provider_mpk_child_tin_dim_ck AS match_input_key,
        'MPK' AS grouper_code,
        a.provider_mpk_id AS grouper_id,
        a.entity_name AS grouper_id_desc,
        CAST(NULL AS VARCHAR(72)) AS parent_grouper_code,
        CAST(NULL AS VARCHAR(100)) AS parent_grouper_id,
        TRY_TO_DATE(TO_VARCHAR(a.provider_mpk_child_tin_eff_date_dim_ck), 'YYYYMMDD') AS effective_start_date,
        TRY_TO_DATE(TO_VARCHAR(a.provider_mpk_child_tin_end_date_dim_ck), 'YYYYMMDD') AS effective_end_date,
        CASE
            WHEN COALESCE(UPPER(a.mpk_deleted_ind), 'Y') = 'N'
                AND COALESCE(UPPER(a.mpk_active_ind), 'N') = 'Y'
                AND effective_start_date IS NOT NULL
                AND CURRENT_DATE BETWEEN effective_start_date 
                    AND COALESCE(effective_end_date, DATE '9999-12-31')
                AND COALESCE(effective_end_date, DATE '9999-12-31') >= effective_start_date
            THEN 'N'
            ELSE 'Y'
        END AS deleted_ind,
        'Medicaid' AS lob_name,
        1 AS source_priority,
        CASE
            WHEN COALESCE(UPPER(a.mpk_deleted_ind), 'Y') = 'N'
                AND COALESCE(UPPER(a.mpk_active_ind), 'N') = 'Y'
                AND effective_start_date IS NOT NULL
                AND CURRENT_DATE BETWEEN effective_start_date 
                    AND COALESCE(effective_end_date, DATE '9999-12-31')
                AND COALESCE(effective_end_date, DATE '9999-12-31') >= effective_start_date
            THEN 1 
            ELSE 0
        END AS active_score
    FROM {{ source('cat_core', 'provider_contract_entity_temp') }} a
    INNER JOIN reparent r
        ON a.provider_mpk_child_tin_dim_ck = r.level2_id
    WHERE a.provider_mpk_id IS NOT NULL
        AND a.entity_name IS NOT NULL
),

deal_terms_src AS (
    SELECT
        r.level3_wuid AS account_key,
        r.level2_id,
        r.level1_name,
        a.provider_ipa_reporting_name AS match_input_key,
        CASE
            WHEN a.provider_master_ipa_reporting_id IS NOT NULL THEN 'MIPA'
            WHEN a.provider_ipa_reporting_id IS NOT NULL THEN 'IPA'
            ELSE NULL
        END AS grouper_code,
        COALESCE(
            a.provider_master_ipa_reporting_id,
            a.provider_ipa_reporting_id
        ) AS grouper_id,
        COALESCE(
            a.provider_master_ipa_reporting_name,
            a.provider_ipa_reporting_name
        ) AS grouper_id_desc,
        CASE
            WHEN a.provider_master_ipa_reporting_id IS NOT NULL THEN 'MIPA'
            ELSE NULL
        END AS parent_grouper_code,
        a.provider_master_ipa_reporting_id AS parent_grouper_id,
        TRY_CAST(a.contract_serv_fund_eff_date AS DATE) AS effective_start_date,
        TRY_CAST(a.contract_serv_fund_term_date AS DATE) AS effective_end_date,
        CASE
            WHEN effective_start_date IS NOT NULL
                AND CURRENT_DATE BETWEEN effective_start_date 
                    AND COALESCE(effective_end_date, DATE '9999-12-31')
                AND COALESCE(effective_end_date, DATE '9999-12-31') >= effective_start_date
            THEN 'N'
            ELSE 'Y'
        END AS deleted_ind,
        'Medicare' AS lob_name,
        2 AS source_priority,
        CASE
            WHEN effective_start_date IS NOT NULL
                AND CURRENT_DATE BETWEEN effective_start_date 
                    AND COALESCE(effective_end_date, DATE '9999-12-31')
                AND COALESCE(effective_end_date, DATE '9999-12-31') >= effective_start_date
            THEN 1 
            ELSE 0
        END AS active_score
    FROM {{ source('cat_core', 'contract_deal_terms') }} a
    INNER JOIN reparent r
        ON a.provider_ipa_reporting_name = r.level1_name
    WHERE COALESCE(a.provider_master_ipa_reporting_id, a.provider_ipa_reporting_id) IS NOT NULL
        AND COALESCE(a.provider_master_ipa_reporting_name, a.provider_ipa_reporting_name) IS NOT NULL
),

unified_src AS (
    SELECT * FROM entity_src
    UNION ALL
    SELECT * FROM deal_terms_src
),

ranked_records AS (
    SELECT
        ABS(HASH(
            COALESCE(CAST(account_key AS VARCHAR), '') || '|' || 
            COALESCE(CAST(grouper_id AS VARCHAR), '') || '|' ||
            COALESCE(CAST(lob_name AS VARCHAR), '')
        )) AS account_vrc_grouper_sk,
        account_key,
        lob_name,
        grouper_code,
        grouper_id,
        grouper_id_desc,
        parent_grouper_code,
        parent_grouper_id,
        deleted_ind,
        -- Include for debugging/audit if needed
        -- match_input_key,
        -- level2_id,
        -- level1_name,
        -- effective_start_date,
        -- effective_end_date,
        source_priority,
        active_score,
        ROW_NUMBER() OVER (
            PARTITION BY account_key
            ORDER BY
                source_priority ASC,          -- Entity first, then deal terms
                active_score DESC,            -- Active records first
                effective_start_date DESC,    -- Most recent effective date
                COALESCE(effective_end_date, DATE '9999-12-31') DESC,
                grouper_code                  -- Tie-breaker
        ) AS rn
    FROM unified_src
    WHERE account_key IS NOT NULL
        AND grouper_id IS NOT NULL
        AND grouper_code IS NOT NULL
)

SELECT
    account_vrc_grouper_sk,
    account_key,
    lob_name,
    grouper_code,
    grouper_id,
    grouper_id_desc,
    parent_grouper_code,
    parent_grouper_id,
    deleted_ind,
    CURRENT_TIMESTAMP() AS dbt_loaded_at
FROM ranked_records
WHERE rn = 1
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY account_vrc_grouper_sk
    ORDER BY account_key, grouper_id
) = 1
ORDER BY account_key, lob_name, grouper_code