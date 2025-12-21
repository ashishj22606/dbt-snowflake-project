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
        level2_id,
        level2_name,
        level2_wuid,
        level1_wuid,
        level1_name
    FROM {{ref('DBT_STAGE_MERGE_REPARENT_UUID')}}
),

entity_src AS (
    SELECT
        r.level1_uuid AS account_key,
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
        'Medicaid' AS lob_name
    FROM {{ source('CAT_CORE_ACCESS', 'UTILITY_PROVIDER_CONTRACT_ENTITY_TEMP')}} a
    INNER JOIN reparent r
    ON a.provider_mpk_child_tin_dim_ck = r.level2_id
    WHERE a.provider_mpk_id IS NOT NULL
    AND a.entity_name IS NOT NULL
    AND r.level2_id IS NOT NULL
),

deal_terms_src AS (
    -- MIPA row (Level 1) -- only when master MIPA exists
    SELECT
        r.level1_wuid AS account_key,
        r.level1_name,
        a.provider_ipa_reporting_name AS match_input_key, -- join input, still okay
        'MIPA' AS grouper_code, -- per spec
        a.provider_master_ipa_reporting_id AS grouper_id, -- level 1 ID
        a.provider_master_ipa_reporting_name AS grouper_id_desc,
        /* MIPA has no parent */
        CAST(NULL AS VARCHAR(72)) AS parent_grouper_code,
        CAST(NULL AS VARCHAR(108)) AS parent_grouper_id,
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
        'Medicare' AS lob_name
    FROM {{ source('CAT_CORE_ACCESS','UTILITY_CONTRACT_DEAL_TERMS') }} a
    INNER JOIN reparent r
    ON a.PROVIDER_MASTER_IPA_REPORTING_NAME = r.level1_name
    WHERE a.provider_master_ipa_reporting_id IS NOT NULL
    AND a.provider_master_ipa_reporting_name IS NOT NULL
    AND r.level1_name IS NOT NULL
    
    UNION ALL
    
    -- IPA row (Level 2) -- only when IPA exists; parent fields set if a MIPA exists
    SELECT
        r.level2_uuid AS account_key,
        r.level1_name,
        a.provider_ipa_reporting_name AS match_input_key,
        'IPA' AS grouper_code, -- per spec
        a.provider_ipa_reporting_id AS grouper_id, -- Level 2 ID
        a.provider_ipa_reporting_name AS grouper_id_desc,
        /* Parent only if master MIPA exists for this IPA */
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
        'Medicare' AS lob_name
    FROM {{ source('CAT_CORE_ACCESS', 'UTILITY_CONTRACT_DEAL_TERMS') }} a
    INNER JOIN reparent r
    ON a.provider_ipa_reporting_name = r.level2_name
    WHERE a.provider_ipa_reporting_id IS NOT NULL
    AND a.provider_ipa_reporting_name IS NOT NULL
    AND r.level2_name IS NOT NULL
),

unified_src AS (
    SELECT * FROM entity_src
    UNION ALL
    SELECT * FROM deal_terms_src
),

ranked_records AS (
    SELECT
        ABS(HASH(
            COALESCE(CAST(account_key AS VARCHAR), '') || '||' ||
            COALESCE(CAST(grouper_id AS VARCHAR), '')
        )) AS account_vbc_grouper_sk,
        account_key,
        lob_name,
        grouper_code,
        grouper_id,
        grouper_id_desc,
        parent_grouper_code,
        parent_grouper_id,
        deleted_ind
    FROM unified_src
    WHERE account_key IS NOT NULL
    AND grouper_id IS NOT NULL
    AND grouper_code IS NOT NULL
)

SELECT
    account_vbc_grouper_sk,
    account_key,
    lob_name,
    grouper_code,
    grouper_id,
    grouper_id_desc,
    parent_grouper_code,
    parent_grouper_id,
    current_timestamp::timestamp_ntz as INSERT_INSTP,
    current_timestamp::timestamp_ntz as UPDATE_INSTP,
    CURRENT_USER() as INSERT_USER_ID,
    CURRENT_USER() as UPDATE_USER_ID,
    deleted_ind,
    'PDM_PROVIDER' as SOURCE_SYSTEM_CODE,
    current_timestamp()::timestamp_ntz as SOURCE_INSERT_INSTP,
    current_timestamp()::timestamp_ntz as SOURCE_UPDATE_INSTP,
    null::varchar(30) as UPDATED_SOURCE_SYSTEM_CODE,
    null::varchar(30) as OPERATION_TYPE_CODE
FROM ranked_records
{% if is_incremental() %}
WHERE UPDATE_INSTP > (SELECT MAX(UPDATE_INSTP) FROM {{ this }})
OR INSERT_INSTP > (SELECT MAX(INSERT_INSTP) FROM {{ this }})
{% endif %}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY account_vbc_grouper_sk
    ORDER BY account_key, grouper_id
) = 1
ORDER BY account_key, lob_name, grouper_code