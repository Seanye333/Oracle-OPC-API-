-- ============================================================
-- PL/SQL Package: PKG_P6_PIPELINE
-- MERGE (upsert) procedures for all P6 pipeline tables
-- Called by python-oracledb executemany in oracle_loader.py
-- ============================================================

CREATE OR REPLACE PACKAGE P6_DATA.PKG_P6_PIPELINE AS

    PROCEDURE UPSERT_PROJECT(
        p_object_id          IN NUMBER,
        p_project_id         IN VARCHAR2,
        p_project_name       IN VARCHAR2,
        p_status             IN VARCHAR2,
        p_start_date         IN DATE,
        p_finish_date        IN DATE,
        p_planned_start      IN DATE,
        p_planned_finish     IN DATE,
        p_data_date          IN DATE,
        p_total_budgeted     IN NUMBER,
        p_actual_total_cost  IN NUMBER,
        p_ev_cost            IN NUMBER
    );

    PROCEDURE UPSERT_WBS(
        p_object_id          IN NUMBER,
        p_project_object_id  IN NUMBER,
        p_wbs_code           IN VARCHAR2,
        p_wbs_name           IN VARCHAR2,
        p_parent_object_id   IN NUMBER,
        p_sequence_number    IN NUMBER,
        p_total_budgeted     IN NUMBER,
        p_actual_total_cost  IN NUMBER
    );

    PROCEDURE UPSERT_ACTIVITY(
        p_object_id          IN NUMBER,
        p_project_object_id  IN NUMBER,
        p_wbs_object_id      IN NUMBER,
        p_activity_id        IN VARCHAR2,
        p_activity_name      IN VARCHAR2,
        p_activity_type      IN VARCHAR2,
        p_status             IN VARCHAR2,
        p_planned_start      IN DATE,
        p_planned_finish     IN DATE,
        p_actual_start       IN DATE,
        p_actual_finish      IN DATE,
        p_planned_cost       IN NUMBER,
        p_actual_cost        IN NUMBER,
        p_bac                IN NUMBER,
        p_ev_cost            IN NUMBER,
        p_planned_duration   IN NUMBER,
        p_percent_complete   IN NUMBER
    );

    PROCEDURE UPSERT_RESOURCE(
        p_object_id          IN NUMBER,
        p_resource_id        IN VARCHAR2,
        p_resource_name      IN VARCHAR2,
        p_resource_type      IN VARCHAR2,
        p_price_per_unit     IN NUMBER,
        p_unit_of_measure    IN VARCHAR2,
        p_is_active          IN NUMBER,
        p_max_units          IN NUMBER,
        p_default_units      IN NUMBER
    );

    PROCEDURE UPSERT_RESOURCE_ASSIGNMENT(
        p_object_id          IN NUMBER,
        p_activity_object_id IN NUMBER,
        p_project_object_id  IN NUMBER,
        p_resource_object_id IN NUMBER,
        p_role_object_id     IN NUMBER,
        p_run_date           IN DATE,
        p_planned_units      IN NUMBER,
        p_actual_units       IN NUMBER,
        p_remaining_units    IN NUMBER,
        p_planned_cost       IN NUMBER,
        p_actual_cost        IN NUMBER,
        p_remaining_cost     IN NUMBER,
        p_utilization_pct    IN NUMBER,
        p_unit_variance      IN NUMBER,
        p_cost_variance      IN NUMBER,
        p_over_allocated     IN NUMBER,
        p_planned_start      IN DATE,
        p_planned_finish     IN DATE,
        p_actual_start       IN DATE
    );

    PROCEDURE UPSERT_COST_METRIC(
        p_project_object_id  IN NUMBER,
        p_project_id         IN VARCHAR2,
        p_project_name       IN VARCHAR2,
        p_data_date          IN DATE,
        p_run_date           IN DATE,
        p_pv                 IN NUMBER,
        p_ev                 IN NUMBER,
        p_ac                 IN NUMBER,
        p_bac                IN NUMBER,
        p_cpi                IN NUMBER,
        p_spi                IN NUMBER,
        p_cv                 IN NUMBER,
        p_sv                 IN NUMBER,
        p_eac                IN NUMBER,
        p_vac                IN NUMBER,
        p_tcpi               IN NUMBER,
        p_cv_pct             IN NUMBER,
        p_sv_pct             IN NUMBER,
        p_eac_method         IN VARCHAR2
    );

    PROCEDURE UPSERT_RESOURCE_METRIC(
        p_resource_object_id   IN NUMBER,
        p_run_date             IN DATE,
        p_total_planned_units  IN NUMBER,
        p_total_actual_units   IN NUMBER,
        p_total_remaining_units IN NUMBER,
        p_total_planned_cost   IN NUMBER,
        p_total_actual_cost    IN NUMBER,
        p_total_remaining_cost IN NUMBER,
        p_utilization_pct      IN NUMBER,
        p_assignment_count     IN NUMBER,
        p_over_allocated_count IN NUMBER
    );

END PKG_P6_PIPELINE;
/

-- ============================================================
-- Package Body
-- ============================================================
CREATE OR REPLACE PACKAGE BODY P6_DATA.PKG_P6_PIPELINE AS

    -- ----------------------------------------------------------
    PROCEDURE UPSERT_PROJECT(
        p_object_id          IN NUMBER,
        p_project_id         IN VARCHAR2,
        p_project_name       IN VARCHAR2,
        p_status             IN VARCHAR2,
        p_start_date         IN DATE,
        p_finish_date        IN DATE,
        p_planned_start      IN DATE,
        p_planned_finish     IN DATE,
        p_data_date          IN DATE,
        p_total_budgeted     IN NUMBER,
        p_actual_total_cost  IN NUMBER,
        p_ev_cost            IN NUMBER
    ) IS
    BEGIN
        MERGE INTO P6_DATA.P6_PROJECTS tgt
        USING (SELECT p_object_id AS OBJECT_ID FROM DUAL) src
        ON (tgt.OBJECT_ID = src.OBJECT_ID)
        WHEN MATCHED THEN UPDATE SET
            PROJECT_ID          = p_project_id,
            PROJECT_NAME        = p_project_name,
            STATUS              = p_status,
            START_DATE          = p_start_date,
            FINISH_DATE         = p_finish_date,
            PLANNED_START_DATE  = p_planned_start,
            PLANNED_FINISH_DATE = p_planned_finish,
            DATA_DATE           = p_data_date,
            TOTAL_BUDGETED_COST = p_total_budgeted,
            ACTUAL_TOTAL_COST   = p_actual_total_cost,
            EARNED_VALUE_COST   = p_ev_cost,
            LOAD_TIMESTAMP      = SYSTIMESTAMP
        WHEN NOT MATCHED THEN INSERT (
            OBJECT_ID, PROJECT_ID, PROJECT_NAME, STATUS,
            START_DATE, FINISH_DATE, PLANNED_START_DATE, PLANNED_FINISH_DATE,
            DATA_DATE, TOTAL_BUDGETED_COST, ACTUAL_TOTAL_COST, EARNED_VALUE_COST,
            LOAD_TIMESTAMP
        ) VALUES (
            p_object_id, p_project_id, p_project_name, p_status,
            p_start_date, p_finish_date, p_planned_start, p_planned_finish,
            p_data_date, p_total_budgeted, p_actual_total_cost, p_ev_cost,
            SYSTIMESTAMP
        );
    END UPSERT_PROJECT;

    -- ----------------------------------------------------------
    PROCEDURE UPSERT_WBS(
        p_object_id          IN NUMBER,
        p_project_object_id  IN NUMBER,
        p_wbs_code           IN VARCHAR2,
        p_wbs_name           IN VARCHAR2,
        p_parent_object_id   IN NUMBER,
        p_sequence_number    IN NUMBER,
        p_total_budgeted     IN NUMBER,
        p_actual_total_cost  IN NUMBER
    ) IS
    BEGIN
        MERGE INTO P6_DATA.P6_WBS tgt
        USING (SELECT p_object_id AS OBJECT_ID FROM DUAL) src
        ON (tgt.OBJECT_ID = src.OBJECT_ID)
        WHEN MATCHED THEN UPDATE SET
            PROJECT_OBJECT_ID   = p_project_object_id,
            WBS_CODE            = p_wbs_code,
            WBS_NAME            = p_wbs_name,
            PARENT_OBJECT_ID    = p_parent_object_id,
            SEQUENCE_NUMBER     = p_sequence_number,
            TOTAL_BUDGETED_COST = p_total_budgeted,
            ACTUAL_TOTAL_COST   = p_actual_total_cost,
            LOAD_TIMESTAMP      = SYSTIMESTAMP
        WHEN NOT MATCHED THEN INSERT (
            OBJECT_ID, PROJECT_OBJECT_ID, WBS_CODE, WBS_NAME,
            PARENT_OBJECT_ID, SEQUENCE_NUMBER,
            TOTAL_BUDGETED_COST, ACTUAL_TOTAL_COST, LOAD_TIMESTAMP
        ) VALUES (
            p_object_id, p_project_object_id, p_wbs_code, p_wbs_name,
            p_parent_object_id, p_sequence_number,
            p_total_budgeted, p_actual_total_cost, SYSTIMESTAMP
        );
    END UPSERT_WBS;

    -- ----------------------------------------------------------
    PROCEDURE UPSERT_ACTIVITY(
        p_object_id          IN NUMBER,
        p_project_object_id  IN NUMBER,
        p_wbs_object_id      IN NUMBER,
        p_activity_id        IN VARCHAR2,
        p_activity_name      IN VARCHAR2,
        p_activity_type      IN VARCHAR2,
        p_status             IN VARCHAR2,
        p_planned_start      IN DATE,
        p_planned_finish     IN DATE,
        p_actual_start       IN DATE,
        p_actual_finish      IN DATE,
        p_planned_cost       IN NUMBER,
        p_actual_cost        IN NUMBER,
        p_bac                IN NUMBER,
        p_ev_cost            IN NUMBER,
        p_planned_duration   IN NUMBER,
        p_percent_complete   IN NUMBER
    ) IS
    BEGIN
        MERGE INTO P6_DATA.P6_ACTIVITIES tgt
        USING (SELECT p_object_id AS OBJECT_ID FROM DUAL) src
        ON (tgt.OBJECT_ID = src.OBJECT_ID)
        WHEN MATCHED THEN UPDATE SET
            PROJECT_OBJECT_ID   = p_project_object_id,
            WBS_OBJECT_ID       = p_wbs_object_id,
            ACTIVITY_ID         = p_activity_id,
            ACTIVITY_NAME       = p_activity_name,
            ACTIVITY_TYPE       = p_activity_type,
            STATUS              = p_status,
            PLANNED_START_DATE  = p_planned_start,
            PLANNED_FINISH_DATE = p_planned_finish,
            ACTUAL_START_DATE   = p_actual_start,
            ACTUAL_FINISH_DATE  = p_actual_finish,
            PLANNED_TOTAL_COST  = p_planned_cost,
            ACTUAL_TOTAL_COST   = p_actual_cost,
            BAC                 = p_bac,
            EARNED_VALUE_COST   = p_ev_cost,
            PLANNED_DURATION    = p_planned_duration,
            PERCENT_COMPLETE    = p_percent_complete,
            LOAD_TIMESTAMP      = SYSTIMESTAMP
        WHEN NOT MATCHED THEN INSERT (
            OBJECT_ID, PROJECT_OBJECT_ID, WBS_OBJECT_ID, ACTIVITY_ID, ACTIVITY_NAME,
            ACTIVITY_TYPE, STATUS, PLANNED_START_DATE, PLANNED_FINISH_DATE,
            ACTUAL_START_DATE, ACTUAL_FINISH_DATE,
            PLANNED_TOTAL_COST, ACTUAL_TOTAL_COST, BAC, EARNED_VALUE_COST,
            PLANNED_DURATION, PERCENT_COMPLETE, LOAD_TIMESTAMP
        ) VALUES (
            p_object_id, p_project_object_id, p_wbs_object_id, p_activity_id, p_activity_name,
            p_activity_type, p_status, p_planned_start, p_planned_finish,
            p_actual_start, p_actual_finish,
            p_planned_cost, p_actual_cost, p_bac, p_ev_cost,
            p_planned_duration, p_percent_complete, SYSTIMESTAMP
        );
    END UPSERT_ACTIVITY;

    -- ----------------------------------------------------------
    PROCEDURE UPSERT_RESOURCE(
        p_object_id          IN NUMBER,
        p_resource_id        IN VARCHAR2,
        p_resource_name      IN VARCHAR2,
        p_resource_type      IN VARCHAR2,
        p_price_per_unit     IN NUMBER,
        p_unit_of_measure    IN VARCHAR2,
        p_is_active          IN NUMBER,
        p_max_units          IN NUMBER,
        p_default_units      IN NUMBER
    ) IS
    BEGIN
        MERGE INTO P6_DATA.P6_RESOURCES tgt
        USING (SELECT p_object_id AS OBJECT_ID FROM DUAL) src
        ON (tgt.OBJECT_ID = src.OBJECT_ID)
        WHEN MATCHED THEN UPDATE SET
            RESOURCE_ID            = p_resource_id,
            RESOURCE_NAME          = p_resource_name,
            RESOURCE_TYPE          = p_resource_type,
            PRICE_PER_UNIT         = p_price_per_unit,
            UNIT_OF_MEASURE        = p_unit_of_measure,
            IS_ACTIVE              = p_is_active,
            MAX_UNITS_PER_TIME     = p_max_units,
            DEFAULT_UNITS_PER_TIME = p_default_units,
            LOAD_TIMESTAMP         = SYSTIMESTAMP
        WHEN NOT MATCHED THEN INSERT (
            OBJECT_ID, RESOURCE_ID, RESOURCE_NAME, RESOURCE_TYPE,
            PRICE_PER_UNIT, UNIT_OF_MEASURE, IS_ACTIVE,
            MAX_UNITS_PER_TIME, DEFAULT_UNITS_PER_TIME, LOAD_TIMESTAMP
        ) VALUES (
            p_object_id, p_resource_id, p_resource_name, p_resource_type,
            p_price_per_unit, p_unit_of_measure, p_is_active,
            p_max_units, p_default_units, SYSTIMESTAMP
        );
    END UPSERT_RESOURCE;

    -- ----------------------------------------------------------
    PROCEDURE UPSERT_RESOURCE_ASSIGNMENT(
        p_object_id          IN NUMBER,
        p_activity_object_id IN NUMBER,
        p_project_object_id  IN NUMBER,
        p_resource_object_id IN NUMBER,
        p_role_object_id     IN NUMBER,
        p_run_date           IN DATE,
        p_planned_units      IN NUMBER,
        p_actual_units       IN NUMBER,
        p_remaining_units    IN NUMBER,
        p_planned_cost       IN NUMBER,
        p_actual_cost        IN NUMBER,
        p_remaining_cost     IN NUMBER,
        p_utilization_pct    IN NUMBER,
        p_unit_variance      IN NUMBER,
        p_cost_variance      IN NUMBER,
        p_over_allocated     IN NUMBER,
        p_planned_start      IN DATE,
        p_planned_finish     IN DATE,
        p_actual_start       IN DATE
    ) IS
    BEGIN
        MERGE INTO P6_DATA.P6_RESOURCE_ASSIGNMENTS tgt
        USING (SELECT p_object_id AS OBJECT_ID, p_run_date AS RUN_DATE FROM DUAL) src
        ON (tgt.OBJECT_ID = src.OBJECT_ID AND tgt.RUN_DATE = src.RUN_DATE)
        WHEN MATCHED THEN UPDATE SET
            PLANNED_UNITS      = p_planned_units,
            ACTUAL_UNITS       = p_actual_units,
            REMAINING_UNITS    = p_remaining_units,
            PLANNED_COST       = p_planned_cost,
            ACTUAL_COST        = p_actual_cost,
            REMAINING_COST     = p_remaining_cost,
            UTILIZATION_PCT    = p_utilization_pct,
            UNIT_VARIANCE      = p_unit_variance,
            COST_VARIANCE      = p_cost_variance,
            OVER_ALLOCATED     = p_over_allocated,
            LOAD_TIMESTAMP     = SYSTIMESTAMP
        WHEN NOT MATCHED THEN INSERT (
            OBJECT_ID, ACTIVITY_OBJECT_ID, PROJECT_OBJECT_ID, RESOURCE_OBJECT_ID,
            ROLE_OBJECT_ID, RUN_DATE, PLANNED_UNITS, ACTUAL_UNITS, REMAINING_UNITS,
            PLANNED_COST, ACTUAL_COST, REMAINING_COST,
            UTILIZATION_PCT, UNIT_VARIANCE, COST_VARIANCE, OVER_ALLOCATED,
            PLANNED_START, PLANNED_FINISH, ACTUAL_START, LOAD_TIMESTAMP
        ) VALUES (
            p_object_id, p_activity_object_id, p_project_object_id, p_resource_object_id,
            p_role_object_id, p_run_date, p_planned_units, p_actual_units, p_remaining_units,
            p_planned_cost, p_actual_cost, p_remaining_cost,
            p_utilization_pct, p_unit_variance, p_cost_variance, p_over_allocated,
            p_planned_start, p_planned_finish, p_actual_start, SYSTIMESTAMP
        );
    END UPSERT_RESOURCE_ASSIGNMENT;

    -- ----------------------------------------------------------
    PROCEDURE UPSERT_COST_METRIC(
        p_project_object_id  IN NUMBER,
        p_project_id         IN VARCHAR2,
        p_project_name       IN VARCHAR2,
        p_data_date          IN DATE,
        p_run_date           IN DATE,
        p_pv                 IN NUMBER,
        p_ev                 IN NUMBER,
        p_ac                 IN NUMBER,
        p_bac                IN NUMBER,
        p_cpi                IN NUMBER,
        p_spi                IN NUMBER,
        p_cv                 IN NUMBER,
        p_sv                 IN NUMBER,
        p_eac                IN NUMBER,
        p_vac                IN NUMBER,
        p_tcpi               IN NUMBER,
        p_cv_pct             IN NUMBER,
        p_sv_pct             IN NUMBER,
        p_eac_method         IN VARCHAR2
    ) IS
    BEGIN
        MERGE INTO P6_DATA.P6_COST_METRICS tgt
        USING (SELECT p_project_object_id AS PROJECT_OBJECT_ID, p_run_date AS RUN_DATE FROM DUAL) src
        ON (tgt.PROJECT_OBJECT_ID = src.PROJECT_OBJECT_ID AND tgt.RUN_DATE = src.RUN_DATE)
        WHEN MATCHED THEN UPDATE SET
            PROJECT_ID        = p_project_id,
            PROJECT_NAME      = p_project_name,
            DATA_DATE         = p_data_date,
            PLANNED_VALUE     = p_pv,
            EARNED_VALUE      = p_ev,
            ACTUAL_COST       = p_ac,
            BAC               = p_bac,
            CPI               = p_cpi,
            SPI               = p_spi,
            COST_VARIANCE     = p_cv,
            SCHEDULE_VARIANCE = p_sv,
            EAC               = p_eac,
            VAC               = p_vac,
            TCPI              = p_tcpi,
            CV_PCT            = p_cv_pct,
            SV_PCT            = p_sv_pct,
            EAC_METHOD        = p_eac_method,
            LOAD_TIMESTAMP    = SYSTIMESTAMP
        WHEN NOT MATCHED THEN INSERT (
            PROJECT_OBJECT_ID, PROJECT_ID, PROJECT_NAME, DATA_DATE, RUN_DATE,
            PLANNED_VALUE, EARNED_VALUE, ACTUAL_COST, BAC,
            CPI, SPI, COST_VARIANCE, SCHEDULE_VARIANCE,
            EAC, VAC, TCPI, CV_PCT, SV_PCT, EAC_METHOD, LOAD_TIMESTAMP
        ) VALUES (
            p_project_object_id, p_project_id, p_project_name, p_data_date, p_run_date,
            p_pv, p_ev, p_ac, p_bac,
            p_cpi, p_spi, p_cv, p_sv,
            p_eac, p_vac, p_tcpi, p_cv_pct, p_sv_pct, p_eac_method, SYSTIMESTAMP
        );
    END UPSERT_COST_METRIC;

    -- ----------------------------------------------------------
    PROCEDURE UPSERT_RESOURCE_METRIC(
        p_resource_object_id   IN NUMBER,
        p_run_date             IN DATE,
        p_total_planned_units  IN NUMBER,
        p_total_actual_units   IN NUMBER,
        p_total_remaining_units IN NUMBER,
        p_total_planned_cost   IN NUMBER,
        p_total_actual_cost    IN NUMBER,
        p_total_remaining_cost IN NUMBER,
        p_utilization_pct      IN NUMBER,
        p_assignment_count     IN NUMBER,
        p_over_allocated_count IN NUMBER
    ) IS
    BEGIN
        MERGE INTO P6_DATA.P6_RESOURCE_METRICS tgt
        USING (SELECT p_resource_object_id AS RESOURCE_OBJECT_ID, p_run_date AS RUN_DATE FROM DUAL) src
        ON (tgt.RESOURCE_OBJECT_ID = src.RESOURCE_OBJECT_ID AND tgt.RUN_DATE = src.RUN_DATE)
        WHEN MATCHED THEN UPDATE SET
            TOTAL_PLANNED_UNITS     = p_total_planned_units,
            TOTAL_ACTUAL_UNITS      = p_total_actual_units,
            TOTAL_REMAINING_UNITS   = p_total_remaining_units,
            TOTAL_PLANNED_COST      = p_total_planned_cost,
            TOTAL_ACTUAL_COST       = p_total_actual_cost,
            TOTAL_REMAINING_COST    = p_total_remaining_cost,
            OVERALL_UTILIZATION_PCT = p_utilization_pct,
            ASSIGNMENT_COUNT        = p_assignment_count,
            OVER_ALLOCATED_COUNT    = p_over_allocated_count,
            LOAD_TIMESTAMP          = SYSTIMESTAMP
        WHEN NOT MATCHED THEN INSERT (
            RESOURCE_OBJECT_ID, RUN_DATE,
            TOTAL_PLANNED_UNITS, TOTAL_ACTUAL_UNITS, TOTAL_REMAINING_UNITS,
            TOTAL_PLANNED_COST, TOTAL_ACTUAL_COST, TOTAL_REMAINING_COST,
            OVERALL_UTILIZATION_PCT, ASSIGNMENT_COUNT, OVER_ALLOCATED_COUNT, LOAD_TIMESTAMP
        ) VALUES (
            p_resource_object_id, p_run_date,
            p_total_planned_units, p_total_actual_units, p_total_remaining_units,
            p_total_planned_cost, p_total_actual_cost, p_total_remaining_cost,
            p_utilization_pct, p_assignment_count, p_over_allocated_count, SYSTIMESTAMP
        );
    END UPSERT_RESOURCE_METRIC;

END PKG_P6_PIPELINE;
/

COMMIT;
