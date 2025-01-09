using System;
using System.Data.Common;
using MPI;
using Npgsql;

public class Conn
{
    String connString;
    NpgsqlConnection conn;

    public Conn(int port, string db, string username)
    {
        connString = string.Format("Host=localhost; Port={0}; Database={1}; User Id={2}; Password=; Include Error Detail=true;", port, db, username);
        conn = new NpgsqlConnection(connString);
    }

    public List<T> Read<T>(string query, Func<DbDataReader, T> maker) {
        using NpgsqlCommand cmd = new NpgsqlCommand(query, conn);
        using DbDataReader reader = cmd.ExecuteReader();

        List<T> results = new List<T>();

        while(reader.Read()) {
            results.Add(maker(reader));
        }

        return results;
    }

    public int Execute(string query) {
        using NpgsqlCommand cmd = new NpgsqlCommand(query, conn);
        return cmd.ExecuteNonQuery();
    }

    public void Open() {
        conn.Open();
    }

    public void Close() {
        conn.Close();
    }
}
