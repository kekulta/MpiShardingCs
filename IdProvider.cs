using System;
using System.Data.Common;
using MPI;
using Npgsql;

public class IdProvider {

    private string createString = "CREATE TABLE IF NOT EXISTS id_provider (id BIGSERIAL PRIMARY KEY, shardnum integer);";

    private string getBatchString = "INSERT INTO id_provider (shardnum) VALUES({0}) RETURNING id;";

    private int batchSize = 100_000;
    private int currentId = 0;
    private int currentBatch = 0;
    private int shard = 0;

    private Conn conn;

    public IdProvider(Conn conn, int shard) {
        this.conn = conn;
        this.shard = shard;

        EnsureTableCreated();
        UpdateBatch();
    }

    public int GetId() {
        int id = currentId++;
        if(currentId == batchSize) {
            UpdateBatch();
            currentId = 0;
        }

        int newId = id + (currentBatch * batchSize);

        return newId;
    }

    private void UpdateBatch() {
        currentBatch = conn.Read(string.Format(getBatchString, shard), (r => readInt(r, "id")))[0];
        Console.WriteLine("On shard {0} new batch requested: {1}", shard, currentBatch);
    }

    private void EnsureTableCreated() {
        conn.Execute(createString);
    }

    private int readInt(DbDataReader reader, string name) {
        return tryParse(reader[name]?.ToString()) ?? throw new Exception("Invalid Result");
    }

    private static int? tryParse(string? c) {
        try {
            return Int32.Parse(c ?? "-1");
        } catch(Exception) {
            return null;
        }
    }
}
