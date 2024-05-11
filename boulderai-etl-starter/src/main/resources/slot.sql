SELECT * FROM pg_replication_slots;

-- 创建复制槽
-- SELECT pg_create_logical_replication_slot('slot_name', 'output_plugin');
-- 创建名为pg_sync_neo4j复制槽，并指定pgoutput作为输出插件
SELECT pg_create_logical_replication_slot('pg_sync_neo4j', 'pgoutput');
-- 删除逻辑复制槽
-- SELECT pg_drop_replication_slot('slot_name');
SELECT pg_drop_replication_slot('pg_sync_neo4j');

-- 创建发布
CREATE PUBLICATION name
    [ FOR ALL TABLES
      | FOR publication_object [, ... ] ]
    [ WITH ( publication_parameter [= value] [, ... ] ) ]

where publication_object is one of:

    TABLE [ ONLY ] table_name [ * ] [ ( column_name [, ... ] ) ] [ WHERE ( expression ) ] [, ... ]
    TABLES IN SCHEMA { schema_name | CURRENT_SCHEMA } [, ... ]

    --demo
    CREATE PUBLICATION mypublication FOR TABLE users, departments;

    CREATE PUBLICATION active_departments FOR TABLE departments WHERE (active IS TRUE);

    CREATE PUBLICATION alltables FOR ALL TABLES;

CREATE PUBLICATION insert_only FOR TABLE mydata
    WITH (publish = 'insert');

CREATE PUBLICATION production_publication FOR TABLE users, departments, TABLES IN SCHEMA production;
CREATE PUBLICATION sales_publication FOR TABLES IN SCHEMA marketing, sales;
CREATE PUBLICATION users_filtered FOR TABLE users (user_id, firstname);

--查询变更列表
SELECT * FROM pg_logical_slot_get_changes('slot', NULL, NULL);

-- 查询对应的发布
SELECT * FROM pg_publication_tables;
-- 创建
CREATE PUBLICATION pg_sync_neo4j_publication_da_entity_attribute FOR TABLE public.da_entity_attribute ;
CREATE PUBLICATION pg_sync_neo4j_publication_md_material_base_info FOR TABLE master_data.md_material_base_info ;

-- 删除
DROP PUBLICATION cidade_pub;

DROP PUBLICATION [ IF EXISTS ] name [, ...] [ CASCADE | RESTRICT ]





pgwal:
  pipeWorkCount: 10
  slotName: metabase_slot_ccc
  waitMin: 100
  walReadTimeout: 20000
  mqTopic: topic.metabase.pgwal
  mqQueue: queue.metabase.pgwal
  notify-on-data-error: true
  use-new-relation: true
  wal-filter-table: true
  wal-subscribe-schema: public,tp,master_data
  clear-all-neo4j-on-start: false
  field-data-availability-can-update: false
  exclude-tables: sc_warehouse_period_snapshot,isc_bom_20230510,isc_bom_20230511
  save-integration-data: true
  check-pg-neo4j-rec-count: false
  log-record-cql: false
  show-data-log: true
  dingding-notify: false
  real-project-name: zhongkong


