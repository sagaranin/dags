
-- metadata table
CREATE TABLE IF NOT EXISTS public.tables_md (
	id serial NOT NULL,
	src_schema varchar NOT NULL,
	src_table varchar NOT NULL,
	fields varchar NOT NULL,
	key_field varchar NOT NULL,
	tgt_schema varchar NOT NULL,
	tgt_table varchar NOT NULL,
	load_type bpchar(1) NOT NULL,
	use_conditions bool NOT NULL DEFAULT false,
	conditions varchar NULL,
	CONSTRAINT load_type_constraint CHECK ((load_type = ANY (ARRAY['f'::bpchar, 'i'::bpchar]))),
	CONSTRAINT tables_md_pk PRIMARY KEY (id)
);


-- stage tables
CREATE TABLE IF NOT EXISTS public.stg_table1 (
	id int4 NOT NULL,
	firstname varchar NULL,
	lastname varchar NULL,
	city varchar NULL,
	created_at timestamp NULL,
	uuid varchar NULL
);

CREATE TABLE IF NOT EXISTS public.stg_table2 (
	id int4 NOT NULL,
	firstname varchar NULL,
	lastname varchar NULL,
	city varchar NULL,
	created_at timestamp NULL,
	uuid varchar NULL
);

CREATE TABLE IF NOT EXISTS public.stg_table3 (
	id int4 NOT NULL,
	firstname varchar NULL,
	lastname varchar NULL,
	city varchar NULL,
	created_at timestamp NULL,
	uuid varchar NULL
);

CREATE TABLE IF NOT EXISTS public.stg_table4 (
	id int4 NOT NULL,
	firstname varchar NULL,
	lastname varchar NULL,
	city varchar NULL,
	created_at timestamp NULL,
	uuid varchar NULL
);

CREATE TABLE IF NOT EXISTS public.stg_table5 (
	id int4 NOT NULL,
	firstname varchar NULL,
	lastname varchar NULL,
	city varchar NULL,
	created_at timestamp NULL,
	uuid varchar NULL
);

CREATE TABLE IF NOT EXISTS public.stg_table6 (
	id int4 NOT NULL,
	firstname varchar NULL,
	lastname varchar NULL,
	city varchar NULL,
	created_at timestamp NULL,
	uuid varchar NULL
);

CREATE TABLE IF NOT EXISTS public.stg_table7 (
	id int4 NOT NULL,
	firstname varchar NULL,
	lastname varchar NULL,
	city varchar NULL,
	created_at timestamp NULL,
	uuid varchar NULL
);

CREATE TABLE IF NOT EXISTS public.stg_table8 (
	id int4 NOT NULL,
	firstname varchar NULL,
	lastname varchar NULL,
	city varchar NULL,
	created_at timestamp NULL,
	uuid varchar NULL
);

-- data tables 

CREATE TABLE IF NOT EXISTS public.table1 (
	id int4 NOT NULL,
	firstname varchar NULL,
	lastname varchar NULL,
	city varchar NULL,
	created_at timestamp NULL,
	uuid varchar NULL,
	CONSTRAINT table1_pk PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.table2 (
	id int4 NOT NULL,
	firstname varchar NULL,
	lastname varchar NULL,
	city varchar NULL,
	created_at timestamp NULL,
	uuid varchar NULL,
	CONSTRAINT table2_pk PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.table3 (
	id int4 NOT NULL,
	firstname varchar NULL,
	lastname varchar NULL,
	city varchar NULL,
	created_at timestamp NULL,
	uuid varchar NULL,
	CONSTRAINT table3_pk PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.table4 (
	id int4 NOT NULL,
	firstname varchar NULL,
	lastname varchar NULL,
	city varchar NULL,
	created_at timestamp NULL,
	uuid varchar NULL,
	CONSTRAINT table4_pk PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.table5 (
	id int4 NOT NULL,
	firstname varchar NULL,
	lastname varchar NULL,
	city varchar NULL,
	created_at timestamp NULL,
	uuid varchar NULL,
	CONSTRAINT table5_pk PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.table6 (
	id int4 NOT NULL,
	firstname varchar NULL,
	lastname varchar NULL,
	city varchar NULL,
	created_at timestamp NULL,
	uuid varchar NULL,
	CONSTRAINT table6_pk PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.table7 (
	id int4 NOT NULL,
	firstname varchar NULL,
	lastname varchar NULL,
	city varchar NULL,
	created_at timestamp NULL,
	uuid varchar NULL,
	CONSTRAINT table7_pk PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.table8 (
	id int4 NOT NULL,
	firstname varchar NULL,
	lastname varchar NULL,
	city varchar NULL,
	created_at timestamp NULL,
	uuid varchar NULL,
	CONSTRAINT table8_pk PRIMARY KEY (id)
);
