# !/bin/bash

# varnames
export POSTGRES_USER=forest_fire_user
export POSTGRES_PASSWORD=forest_fire_password
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=forest_fire_database

# docker pull postgres
docker run --name postgres_database -e POSTGRES_USER=$POSTGRES_USER -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD -e POSTGRES_DB=$POSTGRES_DB -p $POSTGRES_PORT:$POSTGRES_PORT -v pg_data:/var/lib/postgresql/data -d postgres

# Si quieres entrar al contenedor
# docker exec -it postgres_database bash

# Dentro del contenedor ejecutas el siguiente comando para entrar a la base de datos
# psql -U $POSTGRES_USER --db $POSTGRES_DB --password

# Eso te pedira la contraseña que debes ingresar de forma manual y ya estarias en la base de datos
# \dt: con este comando listas las bases de datos

# Puedes usar sentencias SQL pero para que se ejecuten deben terminar en ;