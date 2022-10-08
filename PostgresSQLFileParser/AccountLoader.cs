using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PostgresSQLFileParser
{
    public class AccountLoader : LoaderBase
    {
        public override string From => "COPY postgres_air.account (account_id, login, first_name, last_name, frequent_flyer_id, update_ts) FROM stdin;";

        public override string To => "\\.";

        public override void GenerateInsert(List<string> lines)
        {
            string sql = "insert into account (account_id, login, first_name, last_name, frequent_flyer_id, update_ts) values";

            foreach (var line in lines)
            {
                var values = line.Split('\t');

                sql += $"({values[0]}, '{values[1].Replace('\'', '_')}', '{values[2].Replace('\'', ' ')}', '{values[3].Replace('\'', ' ')}', {GetValOrNull(values[4])}, '{values[5]}'),";
            }

            SQLExecutor.Run(sql.TrimEnd(','));
        }
    }


    public class FlightLoader : LoaderBase
    {
        public override string From => "COPY postgres_air.flight (flight_id, flight_no, scheduled_departure, scheduled_arrival, departure_airport, arrival_airport, status, aircraft_code, actual_departure, actual_arrival, update_ts) FROM stdin;";

        public override string To => "\\.";

        public override int GroupBy => 2000;

        public override void GenerateInsert(List<string> lines)
        {
            string sql = "insert into flight (flight_id, flight_no, scheduled_departure, scheduled_arrival, departure_airport, arrival_airport, status, aircraft_code, actual_departure, actual_arrival, update_ts) values";

            //126148	1554	2020-07-01 12:30:00-05	2020-07-01 13:30:00-05	CLT	TYS	Canceled	CR2	2020-07-01 13:12:17.28-05	2020-07-01 13:31:09.84-05	\N 

            foreach (var line in lines)
            {
                var values = line.Split('\t');

                sql += $"({values[0]}, '{values[1]}', '{values[2]}', '{values[3]}', '{values[4]}', '{values[5]}', '{values[6]}', '{values[7]}', {GetDateTimeValOrNull(values[8])}, {GetDateTimeValOrNull(values[9])}, {GetDateTimeValOrNull(values[10])}),";
            }

            SQLExecutor.Run(sql.TrimEnd(','));
        }
    }

    public class BookingLegLoader : LoaderBase
    {
        public override string From => "COPY postgres_air.booking_leg (booking_leg_id, booking_id, flight_id, leg_num, is_returning, update_ts) FROM stdin;";

        public override string To => "\\.";

        public override int GroupBy => 4000;

        public override void GenerateInsert(List<string> lines)
        {
            string sql = "insert into booking_leg (booking_leg_id, booking_id, flight_id, leg_num, is_returning, update_ts) values";

            foreach (var line in lines)
            {
                var values = line.Split('\t');

                sql += $"({values[0]}, {values[1]}, {values[2]}, {values[3]}, {GetBool(values[4])}, '{values[5]}'),";
            }

            SQLExecutor.Run(sql.TrimEnd(','));
        }
    }

    public class BookingLoader : LoaderBase
    {
        public override string From => "COPY postgres_air.booking (booking_id, booking_ref, booking_name, account_id, email, phone, update_ts, price) FROM stdin;";

        public override string To => "\\.";

        public override int GroupBy => 2000;

        public override void GenerateInsert(List<string> lines)
        {
            string sql = "insert into booking (booking_id, booking_ref, booking_name, account_id, email, phone, update_ts, price) values";

            foreach (var line in lines)
            {
                var values = line.Split('\t');

                sql += $"({values[0]}, '{values[1]}', '{values[2]}', {values[3]}, '{values[4]}', '{values[5]}', '{values[6]}', {GetIntFromDecimal(values[7])}),";
            }

            SQLExecutor.Run(sql.TrimEnd(','));
        }

        private string GetIntFromDecimal(string v)
        {
            float val = float.Parse(v.Replace('.', ','));
            return ((int)val).ToString();
        }
    }

    public class BoardingPassLoader : LoaderBase
    {
        public override string From => "COPY postgres_air.boarding_pass (pass_id, passenger_id, booking_leg_id, seat, boarding_time, precheck, update_ts) FROM stdin;";

        public override string To => "\\.";

        public override int GroupBy => 10000;

        public override void GenerateInsert(List<string> lines)
        {
            string sql = "insert into boarding_pass (pass_id, passenger_id, booking_leg_id, seat, boarding_time, precheck, update_ts) values";

            foreach (var line in lines)
            {
                var values = line.Split('\t');

                sql += $"({values[0]}, {values[1]}, {values[2]}, '{values[3]}', '{values[4]}', {GetBool(values[5])}, '{values[6]}'),";
            }

            SQLExecutor.Run(sql.TrimEnd(','));
        }
    }

    public class AirportLoader : LoaderBase
    {
        public override string From => "COPY postgres_air.airport (airport_code, airport_name, city, airport_tz, continent, iso_country, iso_region, intnl, update_ts) FROM stdin;";

        public override string To => "\\.";

        public override int GroupBy => 100;

        public override void GenerateInsert(List<string> lines)
        {
            string sql = "insert into airport (airport_code, airport_name, city, airport_tz, continent, iso_country, iso_region, intnl) values";

            foreach (var line in lines)
            {
                var values = line.Split('\t');

                sql += $"('{values[0]}', '{values[1].Replace('\'', '_')}', '{values[2].Replace('\'', ' ')}', '{values[3].Replace('\'', ' ')}', '{GetValOrNull(values[4])}', '{values[5]}', '{values[6]}', '{values[7]}'),";
            }

            SQLExecutor.Run(sql.TrimEnd(','));
        }
    }

    public class AircraftLoader : LoaderBase
    {
        public override string From => "COPY postgres_air.aircraft (model, range, class, velocity, code) FROM stdin;";

        public override string To => "\\.";

        public override void GenerateInsert(List<string> lines)
        {
            string sql = "insert into aircraft (model, range, class, velocity, code) values";

            foreach (var line in lines)
            {
                var values = line.Split('\t');

                sql += $"('{values[0]}', {values[1]}, {values[2]}, {values[3]}, '{values[4]}'),";
            }

            SQLExecutor.Run(sql.TrimEnd(','));
        }
    }

    public static class SQLExecutor
    {
        static HttpClient client = new HttpClient();

        public static void Run(string sql)
        {
            int attempt = 0;
            var response = client.PostAsync("http://localhost:5000/sql", new StringContent(sql)).Result;
            while (!response.IsSuccessStatusCode && attempt < 3)
            {
                attempt++;
                response = client.PostAsync("http://localhost:5000/sql", new StringContent(sql)).Result;
            }
        }
    }

    public abstract class LoaderBase
    {
        public abstract string From { get; }
        public abstract string To { get; }
        public abstract void GenerateInsert(List<string> values);
        public virtual int GroupBy { get; } = 2000;

        public void Load(IEnumerator<string> lines, int? limit = null)
        {
            SkipUntil(lines, From);
            ReadUntil(lines, To, limit);
        }

        private void SkipUntil(IEnumerator<string> lines, string expected)
        {
            while (lines.MoveNext())
            {
                var line = lines.Current;
                if (line == expected)
                    return;
            }

            throw new Exception("expected string not found");
        }

        private void ReadUntil(IEnumerator<string> lines, string expected, int? limit)
        {
            List<string> buffer = new List<string>();
            long count = 0;

            while (lines.MoveNext() && limit.HasValue && count < limit.Value)
            {
                var line = lines.Current;
                if (line == expected)
                {
                    if (buffer.Any())
                        GenerateInsert(buffer);
                    return;
                }

                buffer.Add(line);
                if (buffer.Count == GroupBy)
                {
                    GenerateInsert(buffer);
                    count += buffer.Count;
                    buffer.Clear();
                }
            }
        }

        protected static string GetValOrNull(string v)
        {
            if (v == "\\N")
                return "null";
            else
            {
                if (v.Contains('\''))
                    return v.Replace('\'', ' ');

                return v;
            };
        }

        protected static string GetDateTimeValOrNull(string v)
        {
            if (v == "\\N")
                return "null";
            else
            {
                return $"'{v}'";
            };
        }

        protected static string GetBool(string v)
        {
            return v == "f" ? "0" : "1";
        }
    }
}
