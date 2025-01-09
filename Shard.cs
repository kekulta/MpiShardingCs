using System;
using System.Diagnostics;
using System.Data.Common;
using MPI;
using Npgsql;

public class Shard {
    private static int[] ports = {5433, 5434};
    private static int count = ports.Count();
    private static string username = "kekulta";

    private Conn conn;
    private UserDao dao;
    private Intracommunicator comm;
    private Stopwatch stopWatch = new Stopwatch();

    public Shard(Intracommunicator comm) {
        conn = new Conn(ports[comm.Rank], username);
        dao = new UserDao(conn);
        this.comm = comm;
        conn.Open();
        dao.EnsureTableCreated();

        Console.WriteLine("Process {0} opened connection on port {1} as {2}",
                comm.Rank, ports[comm.Rank], username);
    }

    public void WriteLine(string m) {
        if(comm.Rank == 0) Console.WriteLine(m);
    }

    public void WriteLine(string template, params Object?[] prms) {
        if(comm.Rank == 0) Console.WriteLine(template, prms);
    }

    public void Generate(int n) {
        int excess = n % count;
        int part = n / count;
        if(comm.Rank == 0) part += excess;

        Console.WriteLine("Generating {0} users in {1} process.", part, comm.Rank);
        dao.Insert(dao.Generate(part));
    }

    public int? Min(string field) {
        int? n = dao.Min(field);
        int?[]? data = comm.Gather(n, 0);

        if(data?.Any(e => e == null) != false) return null;

        return data?.Min();
    }

    public int? Max(string field) {
        int? n = dao.Max(field);
        int?[]? data = comm.Gather(n, 0);

        if(data?.Any(e => e == null) != false) return null;

        return data?.Max();
    }

    public List<User> ReadAll() {
        List<User>? users = comm.Gather(dao.ReadAll(), 0)
            ?.SelectMany(x => x)
            ?.ToList();
        return users ?? new List<User>();
    }

    public int CountAll() {
        return comm.Reduce(dao.Count(), Operation<int>.Add, 0);
    }

    public void Truncate() {
        dao.Truncate();
    }

    public string? ReadLine() {
        string? cmd = null;
        comm.Barrier();
        if(comm.Rank == 0) {
            cmd = Console.ReadLine();
        }

        comm.Broadcast(ref cmd, 0);

        return cmd;
    }

    public void Close() {
        conn.Close();
    }

    public void Measured(Action func) {
        try {
            Start();
            func();
        } finally {
            End();
        }
    }

    public T Measured<T>(Func<T> func) {
        try {
            Start();
            return func();
        } finally {
            End();
        }
    }

    public void Start(){
        stopWatch.Start();
        stopWatch.Restart();
    }

    public void End(){
        stopWatch.Stop();
    }

    public void Stat(){
        comm.Barrier();

        TimeSpan ts = stopWatch.Elapsed;
        string elapsedTime = String.Format("{0:00}:{1:00}.{2:00}",
            ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
        Console.WriteLine("Rank: {0}, Runtime: {1}", comm.Rank, elapsedTime);

        comm.Barrier();
    }
}
