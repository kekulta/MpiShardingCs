using System;
using System.Data.Common;
using MPI;
using Npgsql;

public class Program
{
    static void Main(string[] args)
    {
        MPI.Environment.Run(ref args, comm =>
        {
            Shard shard = new Shard(comm);

            while(true) {
                string? cmd = shard.ReadLine();

                if(cmd?.StartsWith("Exit") == true
                        || cmd?.StartsWith("Quit") == true
                        || cmd?.StartsWith("q") == true) {
                    shard.WriteLine("Bye!");
                    break;
                }

                if(cmd?.StartsWith("CountAll") == true) {
                    int count = shard.Measured(() => (shard.CountAll()));
                    shard.WriteLine("All users count: {0}", shard.CountAll());
                    shard.Stat();
                    continue;
                }

                if(cmd?.StartsWith("ReadAll") == true) {
                    List<User> l = shard.Measured(() => shard.ReadAll());
                    shard.WriteLine("All users:");
                    if(comm.Rank == 0) {
                        l.ForEach(f => shard.WriteLine(f.ToString()));
                    }
                    shard.Stat();
                    continue;
                }

                if(cmd?.StartsWith("Min ") == true) {
                    string field;
                    int? n;
                    try {
                        // We could hang here if one of the processes won't throw.
                        field = cmd.Substring(4);
                        n = shard.Measured(() => shard.Min(field));
                    } catch(Exception) {
                        shard.WriteLine("Incorrect comand: {0}", cmd);
                        continue;
                    }

                    if(comm.Rank != 0) { 
                        shard.Stat();
                        continue;
                    }

                    if(n != null) {
                        shard.WriteLine("Min of the field {0} is {1}", field, n);
                        shard.Stat();
                    } else {
                        shard.WriteLine("Can't find min of non-integer field '{0}'.", field);
                    }

                    continue;
                }

                if(cmd?.StartsWith("Max ") == true) {
                    string field;
                    int? n;
                    try {
                        // We could hang here if one of the processes won't throw.
                        field = cmd.Substring(4);
                        n = shard.Measured(() => shard.Max(field));
                    } catch(Exception) {
                        shard.WriteLine("Incorrect comand: {0}", cmd);
                        continue;
                    }

                    if(comm.Rank != 0) { 
                        shard.Stat();
                        continue;
                    }

                    if(n != null) {
                        shard.WriteLine("Max of the field {0} is {1}", field, n);
                        shard.Stat();
                    } else {
                        shard.WriteLine("Can't find max of non-integer field '{0}'.", field);
                    }

                    continue;
                }

                if(cmd?.StartsWith("Generate ") == true) {
                    int n = tryParse(cmd.Substring(9)) ?? -1;

                    if(n == -1) {
                        shard.WriteLine("Can't generate '{0}' users...", cmd.Substring(9));
                    } else {
                        shard.WriteLine("Generating {0} users...", n);
                        shard.Measured(() => shard.Generate(n));
                        shard.Stat();
                    }


                    continue;
                }

                if(cmd?.StartsWith("Truncate") == true) {
                    shard.WriteLine("Truncating table users...");
                    shard.Measured(() => shard.Truncate());
                    shard.Stat();
                    continue;
                }


                shard.WriteLine("Unknown command: {0}", cmd);
                shard.WriteLine(usage());
            }
        });
    }

    private static string usage() {
        return @"
   usage: <cmd> [arg]
           Truncate
           CountAll
           ReadAll
           Generate <count>
           Max <field>
           Min <field>
           Exit | Quit | q";
    }

    private static int? tryParse(string c) {
        try {
            return Int32.Parse(c);
        } catch(Exception) {
            return null;
        }
    }
}
