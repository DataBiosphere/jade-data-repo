### Install

Install [postgres server version 9.6](https://github.com/PostgresApp/PostgresApp/releases/download/v2.2/Postgres-2.2-9.5-9.6-10-11.dmg)

This version defaults to starting postgres server version 11. To change to version 9.6:

1. Open postgres app
2. Stop the server
2. Click the icon on the lower left to show the servers
3. Select Postgres11 and then -
4. Select + and choose version 9.6
5. Start the server


### Create the data repo db and user

`psql -f db/create-data-repo-db`


### Clear the database

`psql -f db/truncate-tables datarepo drmanager`
