
DROP TABLE IF EXISTS job;
CREATE TABLE job (
job_id serial PRIMARY KEY,
start_ts real,
end_ts real,
master_type text,
slave_type text,
num_slaves integer,
job_status text,
job_type integer
);

DROP TABLE IF EXISTS job_languages;
CREATE TABLE job_languages (
job_id integer,
lang text,
count integer,
PRIMARY KEY (job_id,lang)
);

DROP TABLE IF EXISTS job_clusters;
CREATE TABLE job_clusters (
job_id integer,
cluster_id integer,
num_docs integer,
PRIMARY KEY (job_id,cluster_id)
);

DROP TABLE IF EXISTS cluster_keywords;
CREATE TABLE cluster_keywords (
job_id integer,
cluster_id integer,
keywords text,
keyword_weights text,
PRIMARY KEY (job_id,cluster_id)
);

DROP TABLE IF EXISTS cluster_topdocs;
CREATE TABLE cluster_topdocs (
job_id integer,
cluster_id integer,
doc_order_id integer,
top_doc text,
PRIMARY KEY (job_id,cluster_id,doc_order_id)
);

DROP TABLE IF EXISTS job_errors;
CREATE TABLE job_errors (
job_id integer PRIMARY KEY,
err_text text
);

DROP TABLE IF EXISTS file_paths;
CREATE TABLE file_paths (
year_month text,
file_path text,
job_id integer
);

