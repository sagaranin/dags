
-- metadata table
CREATE TABLE IF NOT EXISTS celonis_etl.table_metadata (
	id serial NOT NULL,
	is_active bool NOT NULL DEFAULT false,
	src_query text,
	src_schema varchar,
	src_table varchar,
	fields varchar,
	key_field varchar NOT NULL,
	tgt_schema varchar NOT NULL DEFAULT 'celonis_etl',
	tgt_table varchar NOT NULL,
	load_type bpchar(1) NOT NULL,
	use_conditions bool NOT NULL DEFAULT false,
	conditions varchar NULL,
	author varchar NOT NULL,
	udpated timestamp NOT NULL DEFAULT now(),
	CONSTRAINT load_type_constraint CHECK ((load_type = ANY (ARRAY['f'::bpchar, 'i'::bpchar]))),
	CONSTRAINT tables_md_pk PRIMARY KEY (id)
);


