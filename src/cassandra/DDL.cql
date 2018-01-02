---------------------------------------------
-- SCRIPT DE CRIAÇÃO DE TABELAS NO CASSANDRA
---------------------------------------------

-- CRIAR KEYSPACE
CREATE KEYSPACE sea WITH replication = {'class': 'SimpleStrategy', 'replication_factor':1};

-- TABELA DE EVENTOS
CREATE TABLE sea.events (
    source_type text,
    source_id varint,
    sender_id varint,
    sender_name text,
    sender_influence int,
    category_name text,
    event_desc text,
    lang text,
    reference_count int,
    favorite_count int,
    latitude double,
    longitude double,
    street text,
    neighborhood text,
    city text,
    state text,
    country text,
    location_level int,
    timestamp_ms varint,
    PRIMARY KEY (source_type, source_id)
);

-- TABELA DE CONFIGURAÇÕES
CREATE TABLE sea.app_config (
    config_key text,
    config_value text,
    PRIMARY KEY (config_key)
);

-- TABELA DE CATEGORIAS DE EVENTOS
CREATE TABLE sea.event_category (
    category_name text,
    keywords text,
    PRIMARY KEY (category_name)
);