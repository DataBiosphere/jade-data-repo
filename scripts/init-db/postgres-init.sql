-- create both datarepo and stairway databases
CREATE USER drmanager WITH PASSWORD 'drpasswd';
CREATE DATABASE datarepo;
GRANT ALL PRIVILEGES ON DATABASE datarepo to drmanager;
ALTER DATABASE datarepo OWNER to drmanager;
CREATE DATABASE stairway;
GRANT ALL PRIVILEGES ON DATABASE stairway to drmanager;
ALTER DATABASE stairway OWNER to drmanager;
\c datarepo
CREATE EXTENSION IF NOT EXISTS pgcrypto;
