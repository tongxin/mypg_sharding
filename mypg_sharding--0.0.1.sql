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

-- Metadata tables

-- Node state
CREATE TABLE nodestate (
	node_name text NOT NULL,       -- the node name
	current text NOT NULL,         -- local node state, either ACTIVE or INACTIVE
	epoch integer NOT NULL         -- starting from 1, increased everytime a cluster configuration change occurs 
);

-- Server nodes
CREATE TABLE cluster_nodes (
	node_name text NOT NULL UNIQUE,
	system_id bigint NOT NULL UNIQUE,
	host text NOT NULL,
	port text NOT NULL,
	dbname text NOT NULL,
	UNIQUE (host, port)
);

-- Distributed tables
CREATE TABLE tables (
	relation text PRIMARY KEY,     -- table name
	key_col text,                  -- sharding key column
	modulo integer,                -- maximum number of distributed partitions
	create_sql text NOT NULL       -- sql to create the table
--	create_rules_sql text          -- sql to create rules for shared table
);

CREATE TABLE partitions (
	relation text,                 -- table name
	node_name text,                -- node id for the partition
	r integer                      -- modulo index for the partition
);

create type broadcast_result as (res_msg text, res_err text);

-- Sharding interface functions

CREATE FUNCTION add_node (name_ text, host_ text, port_ text) RETURNS mypg.cluster_nodes AS $$
DECLARE
	node cluster_nodes%ROWTYPE;
	sys_id bigint;
	new_epoch int;
	copy_nodes_msg text := '';
	init_nodestate_msg text := '';
	insert_node_msg text := '';
	copy_tables_msg text := '';
	update_epoch_msg text := '';
	res_msg text;
	res_err text;
BEGIN
	-- Fail if this command is not run at the master node.
	IF NOT mypg.is_master()
	THEN 
		RAISE EXCEPTION 'Only master node can invoke add_node.';
	END IF;

	-- Error if the node is already added.
	IF EXISTS (
		SELECT 1 
		FROM mypg.cluster_nodes
		WHERE node_name = name_ and node_state = 'ACTIVE')
	THEN
		RAISE EXCEPTION 'Node % already exists.', name_;
	END IF;
	
	IF EXISTS (
		SELECT 1 
		FROM mypg.cluster_nodes
		WHERE host_ = host and port_ = port)
	THEN
		RAISE EXCEPTION 'Node exists with host=% and port=%', host_, port_;
	END IF;

	-- Insert new node in the cluster_nodes table. Update master's epoch number.
	INSERT INTO mypg.cluster_nodes (node_name, system_id, host, port)
	VALUES (name_, 0, host_, port_) 
		RETURNING system_id INTO sys_id;

	-- Copy the updated cluster metadata off to the new node.
	UPDATE nodestate
	SET epoch = epoch + 1
		RETURNING epoch INTO new_epoch;
	copy_nodes_msg :=
		format('%s', name_, gen_copy_table_sql(mypg.cluster_nodes));
	init_nodestate_msg :=
		format('INSERT INTO mypg.nodestate (node_name,current,epoc) VALUES (%s, ''ACTIVE'' ,%d);', 
				name_, new_epoch);
	copy_nodes_msg :=
		format('{%s:%s;%s}', name_, copy_nodes_msg, init_nodestate_msg);
	SELECT * INTO res_msg, res_err
	FROM mypg.broadcast(copy_nodes_msg, iso_level => 'READ COMMITTED'); -- needs error handling here
	
	IF res_err IS NOT NULL
	THEN
		RAISE EXCEPTION 'Failed to copy metadata to node %s', name_;
	END IF;

	-- Copy the tables metadata to the new node.
	IF EXISTS (
		SELECT 1 FROM mypg.tables) 
	THEN
		copy_tables_msg :=
			format('%s:%s', name_, gen_copy_table_sql(mypg.tables));
		PERFORM mypg.broadcast(copy_tables_msg);
	END IF;

	-- Update current cluster nodes to include metadata of the new node. 
	FOR node IN 
	SELECT * FROM mypg.cluster_nodes
	WHERE node.node_name <> name_
	LOOP
		insert_node_msg :=
			format('%s%s:INSERT INTO mypg.cluster_nodes (node_name,system_id,host,port) VALUES (%s, %s, %s, %s);', 
		            insert_node_msg, node.node_name, name_, sys_id, host_, port_);
		update_epoch_msg :=
			format('%s%s:UPDATE mypg.nodestate SET epoch = epoch + 1 WHERE node_name = %s RETURNING epoch',
		 			update_epoch_msg, node.node_name, node.node_name);
	END LOOP;
	
	SELECT * INTO res_msg, res_err
	FROM mypg.broadcast(insert_node_msg);

	SELECT * INTO res_msg, res_err
	FROM mypg.broadcast(update_epoch_msg, two_phase => true, iso_level => 'READ COMMITTED');

	SELECT * INTO node FROM mypg.cluster_nodes WHERE node_name = name_;
	RETURN node;
END
$$ LANGUAGE plpgsql;


-- Generate based on information from catalog SQL statement creating this table
CREATE FUNCTION gen_create_table_sql(relation text)
RETURNS text AS 'mypg_sharding' LANGUAGE C STRICT;

-- Reconstruct table attributes for foreign table
CREATE FUNCTION reconstruct_table_attrs(relation regclass)
RETURNS text AS 'mypg_sharding' LANGUAGE C STRICT;

CREATE FUNCTION broadcast(cmds text,
						  ignore_errors bool = false,
						  two_phase bool = false,
						  sequential bool = false,
						  iso_level text = null)
RETURNS TABLE (result text, error text) AS 'mypg_sharding' LANGUAGE C;


-- Check from configuration parameters if node plays role of shardlord
CREATE FUNCTION is_master()
	RETURNS bool AS 'mypg_sharding' LANGUAGE C STRICT;

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

-----------------------------------------------------------------------
-- Some useful views.
-----------------------------------------------------------------------
