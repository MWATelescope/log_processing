DROP TABLE IF EXISTS public.obsdownload_history;
--
-- This script creates a history table for Obsdownload
-- proxy logs to be imported into.
--

DROP TABLE IF EXISTS public.jobs_history;
--
-- This script creates a history table for MWA ASVO
-- v1 and v2 logs to be imported into.
--

DROP TABLE IF EXISTS public.ngas_history;

CREATE TABLE IF NOT EXISTS public.jobs_history
(
    job_type integer NOT NULL,
    job_state integer NOT NULL,
    user_id bigint NOT NULL,
    job_params json,
    error_code integer,
    error_text text COLLATE pg_catalog."default",
    created timestamp without time zone,
    started timestamp without time zone,
    completed timestamp without time zone,
    product json,
    id bigint NOT NULL,
    CONSTRAINT jobs_history_pkey PRIMARY KEY (id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.jobs_history
    OWNER to postgres;
-- Index: expiry_index

-- DROP INDEX IF EXISTS public.expiry_index;

CREATE INDEX IF NOT EXISTS expiry_index
    ON public.jobs_history USING btree
    (completed ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: fki_jobs_history_user_fkey

-- DROP INDEX IF EXISTS public.fki_jobs_history_user_fkey;

CREATE INDEX IF NOT EXISTS fki_jobs_history_user_fkey
    ON public.jobs_history USING btree
    (user_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: jobs_history_unique_index

-- DROP INDEX IF EXISTS public.jobs_history_unique_index;

CREATE UNIQUE INDEX IF NOT EXISTS jobs_history_unique_index
    ON public.jobs_history USING btree
    (job_type ASC NULLS LAST, (job_params::text) COLLATE pg_catalog."default" ASC NULLS LAST, user_id ASC NULLS LAST)
    TABLESPACE pg_default
    WHERE job_state <= 1;
-- Index: jobs_history_jobid_index

-- DROP INDEX IF EXISTS public.jobs_history_jobid_index;

CREATE INDEX IF NOT EXISTS jobs_history_jobid_index
    ON public.jobs_history USING btree
    (id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: jobs_history_users_idx

-- DROP INDEX IF EXISTS public.jobs_history_users_idx;

CREATE INDEX IF NOT EXISTS jobs_history_users_idx
    ON public.jobs_history USING btree
    (user_id ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index: obsdownload_history_obsid_index    

-- DROP INDEX IF EXISTS public.obsdownload_history_obsid_index;

CREATE TABLE IF NOT EXISTS public.obsdownload_history
(
    created timestamp without time zone,
    ip_address text NOT NULL,
    obs_id bigint NOT NULL
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS obsdownload_history_obs_id_index
    ON public.obsdownload_history USING btree
    (obs_id ASC NULLS LAST)
    TABLESPACE pg_default;



CREATE TABLE IF NOT EXISTS public.ngas_history
(
    completed timestamp without time zone,
    obs_id bigint NOT NULL,
    num_files bigint NOT NULL
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS ngas_history_obs_id_index
    ON public.ngas_history USING btree
    (obs_id ASC NULLS LAST)
    TABLESPACE pg_default;
