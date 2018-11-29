
\connect "steemua";

DROP TABLE IF EXISTS "steemua_accounts";
CREATE TABLE "public"."steemua_accounts" (
    "block_num" integer NOT NULL,
    "account_index" integer NOT NULL,
    "account_name" character varying(16) NOT NULL,
    "op_num" integer,
    "timestamp" text,
    "type" smallint NOT NULL,
    CONSTRAINT "steemua_accounts_account_index" PRIMARY KEY ("account_index")
) WITH (oids = false);


DROP TABLE IF EXISTS "steemua_accounts2";
CREATE TABLE "public"."steemua_accounts2" (
    "id" integer NOT NULL,
    "name" character varying(16) NOT NULL,
    "created_at" timestamp NOT NULL,
    "block_num" integer NOT NULL,
    "followers" integer NOT NULL,
    "following" integer NOT NULL,
    "cached_at" timestamp NOT NULL,
    "trust_seed" real DEFAULT '0' NOT NULL,
    "trust_score" real DEFAULT '0' NOT NULL,
    "trust_score_norm" real DEFAULT '0' NOT NULL,
    "trust_score_n" real DEFAULT '0' NOT NULL,
    "sybil" smallint DEFAULT '0' NOT NULL,
    CONSTRAINT "steemua_accounts2_id" PRIMARY KEY ("id")
) WITH (oids = false);

CREATE INDEX "ix_steemua_accounts2_87ea5dfc8b8e384d" ON "public"."steemua_accounts2" USING btree ("id");


DROP TABLE IF EXISTS "steemua_follows";
CREATE TABLE "public"."steemua_follows" (
    "follower" integer NOT NULL,
    "following" integer NOT NULL,
    "state" smallint NOT NULL,
    "block_num" integer NOT NULL,
    CONSTRAINT "steemua_follows_follower_following" PRIMARY KEY ("follower", "following")
) WITH (oids = false);

CREATE INDEX "steemua_follows_block_num" ON "public"."steemua_follows" USING btree ("block_num");

CREATE INDEX "steemua_follows_follower_state" ON "public"."steemua_follows" USING btree ("follower", "state");

CREATE INDEX "steemua_follows_following_state" ON "public"."steemua_follows" USING btree ("following", "state");


DROP TABLE IF EXISTS "steemua_lock";
CREATE TABLE "public"."steemua_lock" (
    "key" character varying(50) NOT NULL,
    "value" boolean NOT NULL,
    "value_int" smallint DEFAULT '0' NOT NULL,
    "value_float" real DEFAULT '0' NOT NULL,
    CONSTRAINT "steemua_lock_key" PRIMARY KEY ("key")
) WITH (oids = false);

CREATE INDEX "ix_steemua_lock_a62f2225bf70bfac" ON "public"."steemua_lock" USING btree ("key");


DROP TABLE IF EXISTS "steemua_ops";
CREATE TABLE "public"."steemua_ops" (
    "block_num" integer NOT NULL,
    "trx_id" character varying(40) NOT NULL,
    "trx_num" smallint DEFAULT '0' NOT NULL,
    "op_num" smallint DEFAULT '0' NOT NULL,
    "timestamp" timestamp NOT NULL,
    "new_account_name" character varying(18),
    "new_account_id" integer,
    "follower" character varying(18),
    "following" character varying(18),
    "what" character varying(20),
    "type" smallint NOT NULL,
    CONSTRAINT "steemua_ops_block_num_trx_id_op_num" PRIMARY KEY ("block_num", "trx_id", "op_num")
) WITH (oids = false);

CREATE INDEX "steemua_ops_block_num" ON "public"."steemua_ops" USING btree ("block_num");
