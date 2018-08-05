/* ------------------------------------------------------------------------
 *
 * mypg_sharding.sql
 *   sharding and clustering utilities for mypg
 *
 * Copyright (c) 2018, Tongxin Bai
 *
 * ------------------------------------------------------------------------
 */
          
-- complain if script is sourced in psql, rather than via CREATE EXTENSION
-- \echo Use "CREATE EXTENSION mypg_sharding" to load this file. \quit

-- We define several GUCs (though user can see them in SHOW if she sets it
-- explicitly even without loaded lib) and have to inform pathman that we want
-- shardman's COPY FROM, so it makes sense to load the lib on server start.
DO $$
BEGIN
-- -- Yes, malicious user might have another extension containing 'pg_shardman'...
-- -- Probably better just call no-op func from the library
	IF strpos(current_setting('shared_preload_libraries'), 'mypg_sharding') = 0 THEN
		RAISE EXCEPTION 'mypg_sharding is required to be loaded via shared_preload_libraries.';
	END IF;
END
$$;

-- This extension defines core data and functions for sharding controllers.

-- The global sharding state is maintained in the following defined tables.

-- The list of cluster nodes for sharding to take place on
CREATE TABLE nodeinfo (
	node_name	text	PRIMARY KEY,
	system_id	bigint	NOT NULL	UNIQUE,
	coninfo		text	NOT NULL,		   -- may require superuser role
);

-- Each shard/node is organized in chunks each of which contains one partition
-- from each of a set of related tables which can be called a table family.
-- Tables from the same table family are sharded in the same way, meaning
-- their partitions reside side by side in the same set of chunks.

CREATE TABLE alltablespaces (
	ts_no		serial	PRIMARY KEY,
	node_name	text	REFERENCES nodeinfo,
	ts			text				-- local tablespace name
	UNIQUE (node_name, ts)
);

CREATE TABLE dtsinfo (
	dts			text,				-- which dts this tablespace belongs to
	dts_idx		int,				-- all tablespaces within a dts form an indexed array
	ts_no		REFERENCES alltablespaces (ts_no),
	UNIQUE (dts, idx),
	UNIQUE (ts_no)
);

-- List of all distributed tables.
CREATE TABLE tableinfo (
	table_name	text	PRIMARY KEY,  -- sharding table's global name
	sharding_key		text,		-- partition key expression
	dts			text,				-- distributed tablespace set to reside in
	create_sql	text       			-- sql to create the table
--	create_rules_sql text          	-- sql to create rules for shared table
);

CREATE TABLE partitioninfo (
	table_name	text	REFERENCES tableinfo,
	relname		text,              	-- Local table name for this partition
	ts_no		int		REFERENCES alltablespaces,
	UNIQUE (table_name, ts_no)
);

-- Make the above config tables dump-able
-- SELECT pg_catalog.pg_extension_config_dump('mypg.cluster_nodes', '');
-- SELECT pg_catalog.pg_extension_config_dump('mypg.tables', '');
-- SELECT pg_catalog.pg_extension_config_dump('mypg.partitions', '');

-- Sharding interface functions

-- Add new node to the sharding cluster. Some constraints include: ...
CREATE FUNCTION add_node (
	newnode_name 	text,
	newnode_coninfo text
)
RETURNS record AS $$
DECLARE
	nodename		text;
	tablename		text;
	createsql		text;
	shardkey		text;
	partname		text;
	shardcount		smallint;
	sys_id			bigint;
	coninfo			text;
	server_opts		text;
	um_opts			text;
	new_server_opts text;
	new_um_opts		text;
	fdws			text = '';
	usms			text = '';
	create_tables	text = '';
	create_partitions text = '';
	create_fdws		text = '';
	table_attrs		text;
	res_res			text;
	res_msg			text;
BEGIN
	-- Error if the node already exists.
	IF EXISTS (
		SELECT 	1 
		FROM 	mypg.nodeinfo
		WHERE 	node_name = newnode_name
	)
	THEN
		RAISE EXCEPTION 'Node % already exists.', newnode_name;
	END IF;

	-- Insert new node in the nodeinfo table.
	INSERT INTO	mypg.nodeinfo
			(node_name,		system_id, 	coninfo)
	VALUES 	(newnode_name, 	0, 			newnode_coninfo);

	-- Validate the accessibility of the new node.
	IF NOT EXISTS (
		SELECT * FROM mypg.remote_exec('SELECT 1;', newnode_coninfo);
	)
	THEN
		RAISE EXCEPTION 'Connection failed with connection string ''%''.', newnode_coninfo;
	END IF;

	-- Retrieve the system id from the new node.
 	sys_id := mypg.remote_exec('SELECT mypg.get_system_id();', newnode_coninfo);
	IF EXISTS (SELECT 1 FROM mypg.nodeinfo WHERE system_id = sys_id)
	THEN
		RAISE EXCEPTION 'Node with system id % already is in the cluster.' sys_id;
	END IF;

	-- Update the node's system_id in the cluster_nodes table.
	UPDATE	mypg.nodeinfo
	SET 	system_id = sys_id
	WHERE	node_name = newnode_name;

	-- Add foreign servers for connection between the new and existing nodes.
	SELECT 	*
	FROM	mypg.conninfo_to_postgres_fdw_opts(conn_string_effective)
	INTO	new_server_opts,
			new_um_opts;
	
	FOR 	nodename, coninfo, server_opts, um_opts IN
	SELECT 	node_name,
			coninfo,
			(mypg.conninfo_to_postgres_fdw_opts(coninfo)).*
	FROM 	mypg.nodeinfo
	WHERE 	node_name <> newnode_name
	LOOP
		-- Create foreign server for new node at all other nodes and servers at new node for all other nodes
		fdws := format('%s%s:CREATE SERVER %s FOREIGN DATA WRAPPER postgres_fdw %s;
			 			  %s:CREATE SERVER %s FOREIGN DATA WRAPPER postgres_fdw %s;',
			 fdws, newnode_name, nodename, server_opts,
			 	   nodename, newnode_name, new_server_opts);

		-- Create user mapping for this servers
		usms := format('%s%s:CREATE USER MAPPING FOR CURRENT_USER SERVER %s %s;
			 			  %s:CREATE USER MAPPING FOR CURRENT_USER SERVER %s %s;',
			 usms, newnode_name, node.node_name, um_opts,
			      node.node_name, newnode_name, new_um_opts);
	END LOOP;
	-- Broadcast command for creating foreign servers
	PERFORM shardman.broadcast(fdws);
	-- Broadcast command for creating user mapping for this servers
	PERFORM shardman.broadcast(usms);

	-- On the new node, create FDWs for all existing distributed tables.
	FOR		tablename, createsql, shardkey, shardcount IN 
	SELECT	t.table_name,
			t.create_sql,
			s.shard_key,
			s.shard_count
	FROM 	mypg.tableinfo		t
	JOIN 	mypg.shardinginfo	s	ON	t.table_name = s.table_name
	LOOP
		create_tables := format('%s{%s:%s}',
			create_tables, newnode_name, createsql);
		create_partitions := format('%s%s:SELECT create_hash_partitions(%L,%L,%L);',
			create_partitions, newnode_name, tablename, shardkey, shardcount);
		SELECT mypg.reconstruct_table_attrs(tablename) INTO table_attrs;

		FOR		nodename, partname IN
		SELECT 	node_name,
				relname,
		FROM	mypg.partitioninfo 
		WHERE	table_name = tablename
	    LOOP
			create_fdws := format(
				'%s%s:SELECT mypg.replace_real_with_foreign(%s, %L, %L);',
				create_fdws, newnode_name, nodename, partname, table_attrs);
		END LOOP;
	END LOOP;

	-- Broadcast create table commands
	PERFORM mypg.broadcast(create_tables);
	-- Broadcast create hash partitions command
	PERFORM mypg.broadcast(create_partitions, iso_level => 'read committed');
	-- Broadcast create foreign table commands
	PERFORM mypg.broadcast(create_fdws);

	RETURN newnode_name;
END
$$ LANGUAGE plpgsql;

-- Replace real partition with foreign one. Real partition is locked to avoid
-- stale writes.
CREATE FUNCTION replace_real_with_foreign(target_srv int, part_name name, table_attrs text)
	RETURNS void AS $$
DECLARE
	srv_name name :=  format('node_%s', target_srv);
	fdw_part_name name := format('%s_fdw', part_name);
BEGIN
	RAISE DEBUG '[SHMN] replace table % with foreign %', part_name, fdw_part_name;
	EXECUTE format('CREATE FOREIGN TABLE %I %s SERVER %s OPTIONS (table_name %L);',
				   fdw_part_name, table_attrs, srv_name, part_name);
	PERFORM replace_hash_partition(part_name::regclass, fdw_part_name::regclass);
	EXECUTE format('TRUNCATE TABLE %I', part_name);
	PERFORM shardman.write_protection_on(part_name::regclass);
END
$$ LANGUAGE plpgsql;


-- Construct postgres_fdw options based on the given connection string
CREATE FUNCTION conninfo_to_postgres_fdw_opts(IN conn_string text,
	OUT server_opts text, OUT um_opts text) RETURNS record AS $$
DECLARE
	conn_string_keywords text[];
	conn_string_vals text[];
	server_opts_first_time_through bool = true;
	um_opts_first_time_through bool = true;
BEGIN
	server_opts := '';
	um_opts := '';
	SELECT * FROM mypg.pq_conninfo_parse(conn_string)
	  INTO conn_string_keywords, conn_string_vals;
	FOR i IN 1..array_upper(conn_string_keywords, 1) LOOP
		IF conn_string_keywords[i] = 'client_encoding' OR
			conn_string_keywords[i] = 'fallback_application_name' THEN
			CONTINUE; /* not allowed in postgres_fdw */
		ELSIF conn_string_keywords[i] = 'user' OR
			conn_string_keywords[i] = 'password' THEN -- user mapping option
			IF NOT um_opts_first_time_through THEN
				um_opts := um_opts || ', ';
			END IF;
			um_opts_first_time_through := false;
			um_opts := um_opts ||
				format('%s %L', conn_string_keywords[i], conn_string_vals[i]);
		ELSE -- server option
			IF NOT server_opts_first_time_through THEN
				server_opts := server_opts || ', ';
			END IF;
			server_opts_first_time_through := false;
			server_opts := server_opts ||
				format('%s %L', conn_string_keywords[i], conn_string_vals[i]);
		END IF;
	END LOOP;

	-- OPTIONS () is syntax error, so add OPTIONS only if we really have opts
	IF server_opts != '' THEN
		server_opts := format(' OPTIONS (%s)', server_opts);
	END IF;
	IF um_opts != '' THEN
		um_opts := format(' OPTIONS (%s)', um_opts);
	END IF;
END $$ LANGUAGE plpgsql STRICT;

-- Parse connection string. This function is used by
-- conninfo_to_postgres_fdw_opts to construct postgres_fdw options list.
CREATE FUNCTION pq_conninfo_parse(IN conninfo text, OUT keys text[], OUT vals text[])
	RETURNS record AS 'mypg_sharding' LANGUAGE C STRICT;

-- Generate based on information from catalog SQL statement creating this table
CREATE FUNCTION gen_create_table_sql(relation text)
RETURNS text AS 'mypg_sharding' LANGUAGE C STRICT;

-- Generate pg_dump'ed sql commands for table copying.
CREATE FUNCTION gen_copy_table_sql(relation text)
RETURNS text AS 'mypg_sharding' LANGUAGE C STRICT;

-- Reconstruct table attributes for foreign table
CREATE FUNCTION reconstruct_table_attrs(relation regclass)
RETURNS text AS 'mypg_sharding' LANGUAGE C STRICT;

CREATE FUNCTION broadcast(cmds text,
						  ignore_errors bool = false,
						  two_phase bool = false,
						  sequential bool = false,
						  iso_level text = null)
RETURNS exec_result AS 'mypg_sharding' LANGUAGE C;

CREATE FUNCTION copy_table_data(rel text, node_name text)
RETURNS exec_result AS 'mypg_sharding' LANGUAGE C STRICT;

-- Check from configuration parameters if node plays role of shardlord
CREATE FUNCTION is_master()
	RETURNS bool AS 'mypg_sharding' LANGUAGE C STRICT;

-- Returns this node's node name
CREATE FUNCTION node_name() RETURNS text
AS 'mypg_sharding' LANGUAGE C STRICT;

-- Disable writes to the partition, if we are not replica. This is handy because
-- we use replication to copy table.
CREATE FUNCTION write_protection_on(part regclass) RETURNS void AS $$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'write_protection' AND
												  tgrelid = part) THEN
		EXECUTE format('CREATE TRIGGER write_protection BEFORE INSERT OR UPDATE OR DELETE ON
					   %I FOR EACH STATEMENT EXECUTE PROCEDURE mypg.deny_access();',
					   part::name);
	END IF;
END
$$ LANGUAGE plpgsql;

-- Enable writes to the partition back again
CREATE FUNCTION write_protection_off(part regclass) RETURNS void AS $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'write_protection' AND
											  tgrelid = part) THEN
		EXECUTE format('DROP TRIGGER write_protection ON %I', part::name);
	END IF;
END
$$ LANGUAGE plpgsql;

-- Trigger procedure prohibiting modification of the table
CREATE FUNCTION deny_access() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'This partition was moved to another node. Run mypg.recover(), if this error persists.';
END
$$ LANGUAGE plpgsql;

-- Returns PostgreSQL system identifier (written in control file)
CREATE FUNCTION get_system_id()
    RETURNS bigint AS 'mypg_sharding' LANGUAGE C STRICT;

-- Initialization 

