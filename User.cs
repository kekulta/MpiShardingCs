using System;
using System.Data.Common;
using MPI;
using Npgsql;

[Serializable]
public record User (
    string Id,
    string FirstName,
    string LastName,
    string Age,
    string Sex,
    string City
);

public class UserDao {
    private Random random = new Random();

    private Conn conn;

    private IdProvider idProvider;

    private List<string> firstNames =
            (new string[] { "aaron", "abdul", "abe", "abel", "abraham", "adam", "adan", "adolfo", "adolph", "adrian", "abby", "abigail", "adele", "adrian" }).ToList();

    private List<string> cities =
            (new string[] { "Kazan", "Moscow", "New-York", "Berlin" }).ToList();

    private List<string> sexes =
            (new string[] { "M", "F" }).ToList();

    private List<string> lastNames =
            (new string[] { "abbott", "acosta", "adams", "adkins", "aguilar" }).ToList();

    private string createString = @"CREATE TABLE IF NOT EXISTS users (
                                id integer PRIMARY KEY,
                                first_name VARCHAR(40),
                                last_name VARCHAR(40),
                                age integer,
                                sex VARCHAR(1),
                                city VARCHAR(40)
                                );";

    public UserDao(Conn conn, IdProvider idProvider) {
        this.conn = conn;
        this.idProvider = idProvider;

        EnsureTableCreated();
    }

    public int EnsureTableCreated() {
        return conn.Execute(createString);
    }

    public int Insert(List<User> users) {
        try {
            conn.Execute("BEGIN;");
            users.ForEach(u => conn.Execute(insertString(u)));
        } catch(Exception) {
            conn.Execute("ROLLBACK;");
            throw;
        }

        return conn.Execute("COMMIT;");
    }

    public List<User> ReadAll() {
        List<User> l = conn.Read("SELECT * FROM users;", mapper);

        return l;
    }

    // Happy SQL Injection and merry hacking!
    public int? Min(string field) {
        int? n = conn.Read(string.Format("SELECT MIN({0}) FROM users;", field), (DbDataReader r) => readInt(r, "min"))[0];
        return n;
    }

    public int? Max(string field) {
        int? n = conn.Read(string.Format("SELECT MAX({0}) FROM users;", field), (DbDataReader r) => readInt(r, "max"))[0];
        return n;
    }

    public int Count() {
        List<int> l = conn.Read("SELECT COUNT(*) FROM users;", (DbDataReader r) => readInt(r, "count"));

        return l[0];
    }

    public int Drop() {
        return conn.Execute("DROP TABLE users;");
    }

    public int Truncate() {
        return conn.Execute("TRUNCATE TABLE users;");
    }

    public List<User> Generate(int n) {

        List<User> l = new List<User>();

        for (int i = 0; i < n; i++) {
            l.Add(new User(idProvider.GetId().ToString(), rand(firstNames), rand(lastNames), random.Next(99).ToString(), rand(sexes), rand(cities)));
        }

        return l;
    }

    private T rand<T>(List<T> l) {
        return l[random.Next(l.Count - 1)];
    }

    private string insertString(User user) {
        return string.Format("INSERT INTO users VALUES ({0}, '{1}', '{2}', {3}, '{4}', '{5}');",
                                user.Id, user.FirstName, user.LastName, user.Age, user.Sex, user.City);
    }

    private int? readIntOrNull(DbDataReader reader, string name) {
        return tryParse(reader[name]?.ToString());
    }

    private int readInt(DbDataReader reader, string name) {
        return tryParse(reader[name]?.ToString()) ?? -1;
    }

    private User mapper (DbDataReader reader) {
        return new User(
                   reader["id"].ToString() ?? "<nonexistent>", 
                   reader["first_name"].ToString() ?? "<nonexistent>", 
                   reader["last_name"].ToString() ?? "<nonexistent>",
                   reader["age"].ToString() ?? "<nonexistent>",
                   reader["sex"].ToString() ?? "<nonexistent>",
                   reader["city"].ToString() ?? "<nonexistent>"
                );
    }

    private static int? tryParse(string? c) {
        try {
            return Int32.Parse(c ?? "-1");
        } catch(Exception) {
            return null;
        }
    }
}
