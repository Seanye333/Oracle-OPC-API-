-- ============================================================
-- Oracle DDL: P6 Pipeline Target Tables
-- Schema: P6_DATA (set search_path or prefix table names)
-- ============================================================

-- Drop tables in reverse dependency order (idempotent re-runs)
BEGIN
    FOR t IN (
        SELECT table_name FROM all_tables
        WHERE owner = 'P6_DATA'
        AND table_name IN (
            'P6_RESOURCE_METRICS','P6_COST_METRICS',
            'P6_RESOURCE_ASSIGNMENTS','P6_ACTIVITIES',
            'P6_WBS','P6_RESOURCES','P6_PROJECTS'
        )
    ) LOOP
        EXECUTE IMMEDIATE 'DROP TABLE P6_DATA.' || t.table_name || ' CASCADE CONSTRAINTS';
    END LOOP;
END;
/

-- ============================================================
-- P6_PROJECTS
-- ============================================================
CREATE TABLE P6_DATA.P6_PROJECTS (
    OBJECT_ID              NUMBER(18)    NOT NULL,
    PROJECT_ID             VARCHAR2(50),
    PROJECT_NAME           VARCHAR2(255),
    STATUS                 VARCHAR2(50),
    START_DATE             DATE,
    FINISH_DATE            DATE,
    PLANNED_START_DATE     DATE,
    PLANNED_FINISH_DATE    DATE,
    DATA_DATE              DATE,
    TOTAL_BUDGETED_COST    NUMBER(18,4),
    ACTUAL_TOTAL_COST      NUMBER(18,4),
    EARNED_VALUE_COST      NUMBER(18,4),
    PLANNED_VALUE_COST     NUMBER(18,4),
    CPI                    NUMBER(10,6),
    SPI                    NUMBER(10,6),
    EAC                    NUMBER(18,4),
    BAC                    NUMBER(18,4),
    SUMMARY_BUDGET         NUMBER(18,4),
    LOAD_TIMESTAMP         TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT PK_P6_PROJECTS PRIMARY KEY (OBJECT_ID)
);

COMMENT ON TABLE  P6_DATA.P6_PROJECTS IS 'Oracle OPC P6 project master records';
COMMENT ON COLUMN P6_DATA.P6_PROJECTS.OBJECT_ID IS 'P6 internal project ObjectId (unique)';
COMMENT ON COLUMN P6_DATA.P6_PROJECTS.CPI IS 'Cost Performance Index (EV/AC)';
COMMENT ON COLUMN P6_DATA.P6_PROJECTS.SPI IS 'Schedule Performance Index (EV/PV)';

-- ============================================================
-- P6_WBS
-- ============================================================
CREATE TABLE P6_DATA.P6_WBS (
    OBJECT_ID              NUMBER(18)    NOT NULL,
    PROJECT_OBJECT_ID      NUMBER(18)    NOT NULL,
    WBS_CODE               VARCHAR2(100),
    WBS_NAME               VARCHAR2(255),
    PARENT_OBJECT_ID       NUMBER(18),
    SEQUENCE_NUMBER        NUMBER(10),
    TOTAL_BUDGETED_COST    NUMBER(18,4),
    ACTUAL_TOTAL_COST      NUMBER(18,4),
    LOAD_TIMESTAMP         TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT PK_P6_WBS PRIMARY KEY (OBJECT_ID),
    CONSTRAINT FK_WBS_PROJECT FOREIGN KEY (PROJECT_OBJECT_ID)
        REFERENCES P6_DATA.P6_PROJECTS(OBJECT_ID) ON DELETE CASCADE
);

CREATE INDEX IDX_WBS_PROJECT ON P6_DATA.P6_WBS(PROJECT_OBJECT_ID);

-- ============================================================
-- P6_ACTIVITIES
-- ============================================================
CREATE TABLE P6_DATA.P6_ACTIVITIES (
    OBJECT_ID              NUMBER(18)    NOT NULL,
    PROJECT_OBJECT_ID      NUMBER(18)    NOT NULL,
    WBS_OBJECT_ID          NUMBER(18),
    ACTIVITY_ID            VARCHAR2(50),
    ACTIVITY_NAME          VARCHAR2(512),
    ACTIVITY_TYPE          VARCHAR2(50),
    STATUS                 VARCHAR2(50),
    PLANNED_START_DATE     DATE,
    PLANNED_FINISH_DATE    DATE,
    ACTUAL_START_DATE      DATE,
    ACTUAL_FINISH_DATE     DATE,
    PLANNED_TOTAL_COST     NUMBER(18,4),
    ACTUAL_TOTAL_COST      NUMBER(18,4),
    BAC                    NUMBER(18,4),
    EARNED_VALUE_COST      NUMBER(18,4),
    PLANNED_DURATION       NUMBER(10,2),
    REMAINING_DURATION     NUMBER(10,2),
    PERCENT_COMPLETE       NUMBER(5,2),
    LOAD_TIMESTAMP         TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT PK_P6_ACTIVITIES PRIMARY KEY (OBJECT_ID),
    CONSTRAINT FK_ACT_PROJECT FOREIGN KEY (PROJECT_OBJECT_ID)
        REFERENCES P6_DATA.P6_PROJECTS(OBJECT_ID) ON DELETE CASCADE
);

CREATE INDEX IDX_ACT_PROJECT ON P6_DATA.P6_ACTIVITIES(PROJECT_OBJECT_ID);
CREATE INDEX IDX_ACT_WBS     ON P6_DATA.P6_ACTIVITIES(WBS_OBJECT_ID);

-- ============================================================
-- P6_RESOURCES
-- ============================================================
CREATE TABLE P6_DATA.P6_RESOURCES (
    OBJECT_ID              NUMBER(18)    NOT NULL,
    RESOURCE_ID            VARCHAR2(50),
    RESOURCE_NAME          VARCHAR2(255),
    RESOURCE_TYPE          VARCHAR2(50),
    PRICE_PER_UNIT         NUMBER(18,4),
    UNIT_OF_MEASURE        VARCHAR2(50),
    IS_ACTIVE              NUMBER(1) DEFAULT 1,
    MAX_UNITS_PER_TIME     NUMBER(10,4),
    DEFAULT_UNITS_PER_TIME NUMBER(10,4),
    PARENT_OBJECT_ID       NUMBER(18),
    ROLE_OBJECT_ID         NUMBER(18),
    LOAD_TIMESTAMP         TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT PK_P6_RESOURCES PRIMARY KEY (OBJECT_ID)
);

-- ============================================================
-- P6_RESOURCE_ASSIGNMENTS
-- ============================================================
CREATE TABLE P6_DATA.P6_RESOURCE_ASSIGNMENTS (
    OBJECT_ID              NUMBER(18)    NOT NULL,
    ACTIVITY_OBJECT_ID     NUMBER(18)    NOT NULL,
    PROJECT_OBJECT_ID      NUMBER(18)    NOT NULL,
    RESOURCE_OBJECT_ID     NUMBER(18),
    ROLE_OBJECT_ID         NUMBER(18),
    RUN_DATE               DATE          NOT NULL,
    PLANNED_UNITS          NUMBER(18,4),
    ACTUAL_UNITS           NUMBER(18,4),
    REMAINING_UNITS        NUMBER(18,4),
    PLANNED_COST           NUMBER(18,4),
    ACTUAL_COST            NUMBER(18,4),
    REMAINING_COST         NUMBER(18,4),
    UTILIZATION_PCT        NUMBER(10,4),
    UNIT_VARIANCE          NUMBER(18,4),
    COST_VARIANCE          NUMBER(18,4),
    OVER_ALLOCATED         NUMBER(1) DEFAULT 0,
    PLANNED_START          DATE,
    PLANNED_FINISH         DATE,
    ACTUAL_START           DATE,
    ACTUAL_FINISH          DATE,
    LOAD_TIMESTAMP         TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT PK_P6_ASSIGNMENTS PRIMARY KEY (OBJECT_ID, RUN_DATE)
);

CREATE INDEX IDX_ASSIGN_PROJ  ON P6_DATA.P6_RESOURCE_ASSIGNMENTS(PROJECT_OBJECT_ID, RUN_DATE);
CREATE INDEX IDX_ASSIGN_RES   ON P6_DATA.P6_RESOURCE_ASSIGNMENTS(RESOURCE_OBJECT_ID, RUN_DATE);

-- ============================================================
-- P6_COST_METRICS  (project-level EV calculations)
-- ============================================================
CREATE TABLE P6_DATA.P6_COST_METRICS (
    PROJECT_OBJECT_ID      NUMBER(18)    NOT NULL,
    RUN_DATE               DATE          NOT NULL,
    PROJECT_ID             VARCHAR2(50),
    PROJECT_NAME           VARCHAR2(255),
    DATA_DATE              DATE,
    PLANNED_VALUE          NUMBER(18,4),
    EARNED_VALUE           NUMBER(18,4),
    ACTUAL_COST            NUMBER(18,4),
    BAC                    NUMBER(18,4),
    CPI                    NUMBER(10,6),
    SPI                    NUMBER(10,6),
    COST_VARIANCE          NUMBER(18,4),
    SCHEDULE_VARIANCE      NUMBER(18,4),
    EAC                    NUMBER(18,4),
    VAC                    NUMBER(18,4),
    TCPI                   NUMBER(10,6),
    CV_PCT                 NUMBER(10,4),
    SV_PCT                 NUMBER(10,4),
    EAC_METHOD             VARCHAR2(30),
    LOAD_TIMESTAMP         TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT PK_P6_COST_METRICS PRIMARY KEY (PROJECT_OBJECT_ID, RUN_DATE)
);

CREATE INDEX IDX_COST_DATE ON P6_DATA.P6_COST_METRICS(RUN_DATE);

-- ============================================================
-- P6_RESOURCE_METRICS  (per-resource aggregated utilization)
-- ============================================================
CREATE TABLE P6_DATA.P6_RESOURCE_METRICS (
    RESOURCE_OBJECT_ID     NUMBER(18)    NOT NULL,
    RUN_DATE               DATE          NOT NULL,
    TOTAL_PLANNED_UNITS    NUMBER(18,4),
    TOTAL_ACTUAL_UNITS     NUMBER(18,4),
    TOTAL_REMAINING_UNITS  NUMBER(18,4),
    TOTAL_PLANNED_COST     NUMBER(18,4),
    TOTAL_ACTUAL_COST      NUMBER(18,4),
    TOTAL_REMAINING_COST   NUMBER(18,4),
    OVERALL_UTILIZATION_PCT NUMBER(10,4),
    ASSIGNMENT_COUNT       NUMBER(10),
    OVER_ALLOCATED_COUNT   NUMBER(10),
    LOAD_TIMESTAMP         TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT PK_P6_RES_METRICS PRIMARY KEY (RESOURCE_OBJECT_ID, RUN_DATE)
);

CREATE INDEX IDX_RES_METRIC_DATE ON P6_DATA.P6_RESOURCE_METRICS(RUN_DATE);

-- ============================================================
-- Grant permissions to pipeline user
-- ============================================================
GRANT SELECT, INSERT, UPDATE, DELETE ON P6_DATA.P6_PROJECTS            TO p6_pipeline_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON P6_DATA.P6_WBS                 TO p6_pipeline_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON P6_DATA.P6_ACTIVITIES           TO p6_pipeline_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON P6_DATA.P6_RESOURCES            TO p6_pipeline_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON P6_DATA.P6_RESOURCE_ASSIGNMENTS TO p6_pipeline_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON P6_DATA.P6_COST_METRICS         TO p6_pipeline_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON P6_DATA.P6_RESOURCE_METRICS     TO p6_pipeline_user;

COMMIT;
/
