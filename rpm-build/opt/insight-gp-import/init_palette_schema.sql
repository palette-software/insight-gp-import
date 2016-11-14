BEGIN;
CREATE SCHEMA 'palette';

# Create GP palette roles
CREATE ROLE palette_palette_looker WITH NOLOGIN;
CREATE ROLE palette_palette_updater WITH NOLOGIN;

# Create GP palette users
CREATE ROLE readonly WITH LOGIN PASSWORD 'onlyread';
CREATE ROLE palette WITH LOGIN PASSWORD 'palette123';
CREATE ROLE palette_etl_user WITH LOGIN PASSWORD 'palette123';
CREATE ROLE palette_extract_user WITH LOGIN PASSWORD 'palette123';

alter role palette with SUPERUSER;
alter user readonly set random_page_cost=20;
alter user readonly set optimizer=on;
alter role palette_etl_user with CREATEEXTTABLE;

grant usage on schema palette to palette_palette_looker;
grant all on schema palette to palette_palette_updater;

grant palette_palette_looker to readonly;
grant palette_palette_looker to palette_extract_user;
grant palette_palette_updater to palette_etl_user;

CREATE RESOURCE QUEUE reporting WITH (ACTIVE_STATEMENTS=10, PRIORITY=MAX);

ALTER ROLE readonly RESOURCE QUEUE reporting;
COMMIT;
