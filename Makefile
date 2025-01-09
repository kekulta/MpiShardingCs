all: bin/Debug/net6.0/Course2
	mpiexec -n 2 bin/Debug/net6.0/Course2

run:
	dotnet run

bin/Debug/net6.0/Course2: *.cs
	dotnet build

build: *.cs
	dotnet build

initdb:
	initdb -D ./shards/shard-1
	echo "unix_socket_directories = '/tmp'" > ./shards/shard-1/postgresql.conf
	initdb -D ./shards/shard-2
	echo "unix_socket_directories = '/tmp'" > ./shards/shard-2/postgresql.conf

createdb:
	createdb -h localhost -p 5433 -U kekulta mpi_db
	createdb -h localhost -p 5434 -U kekulta mpi_db

startdb:
	pg_ctl -D ./shards/shard-1 -o "-p 5433" -l logfile start
	pg_ctl -D ./shards/shard-2 -o "-p 5434" -l logfile start

stopdb:
	pg_ctl -D ./shards/shard-1 -o "-p 5433" -l logfile stop
	pg_ctl -D ./shards/shard-2 -o "-p 5434" -l logfile stop

shard-1:
	psql -h localhost -p 5433 mpi_db

shard-2:
	psql -h localhost -p 5434 mpi_db
