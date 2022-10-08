namespace PostgresSQLFileParser
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var lines = GetAllLines(@"c:\temp\postgres_air.sql");

            float val = float.Parse("213,12");

            SQLExecutor.Run(@"create table account (
    account_id int not null,
    login varchar(32) not null,
    first_name varchar(16) not null,
    last_name varchar(16) not null,
    frequent_flyer_id int,
    update_ts dateTime
)");

            Console.WriteLine("AccountLoader");

        new AccountLoader().Load(lines, 1000);

            SQLExecutor.Run(@"create table aircraft (
    model varchar(32),
    range int not null,
    class int not null,
    velocity int not null,
    code varchar(3) not null
)");

            Console.WriteLine("AircraftLoader");

            new AircraftLoader().Load(lines);

            SQLExecutor.Run(@"create table airport (
    airport_code varchar(3) not null,
    airport_name varchar(64) not null,
    city varchar(64) not null,
    airport_tz varchar(64) not null,
    continent varchar(8),
    iso_country varchar(4),
    iso_region varchar(8),
    intnl varchar(4)
)");

            Console.WriteLine("AirportLoader");
            new AirportLoader().Load(lines);

            SQLExecutor.Run(@"create table boarding_pass (
    pass_id int  not null,
    passenger_id int,
    booking_leg_id int,
    seat varchar(8),
    boarding_time dateTime,
    precheck int,
    update_ts dateTime)");

            Console.WriteLine("BoardingPassLoader");

            new BoardingPassLoader().Load(lines, 1000);

            SQLExecutor.Run(@"create table booking (
    booking_id int not null,
    booking_ref varchar(8) not null,
    booking_name varchar(32),
    account_id int,
    email varchar(32) not null,
    phone varchar(32) not null,
    update_ts dateTime,
    price int
)");

            Console.WriteLine("BookingLoader");

            new BookingLoader().Load(lines, 1000);

            SQLExecutor.Run(@"create table booking_leg (
    booking_leg_id int not null,
    booking_id int not null,
    flight_id int not null,
    leg_num int,
    is_returning int,
    update_ts dateTime
)");


            Console.WriteLine("BookingLegLoader");
            new BookingLegLoader().Load(lines, 1000);

            SQLExecutor.Run(@"create table flight (
    flight_id int not null,
    flight_no varchar(8) not null,
    scheduled_departure dateTime not null,
    scheduled_arrival dateTime not null,
    departure_airport varchar(3) not null,
    arrival_airport varchar(3) not null,
    status varchar(8) not null,
    aircraft_code varchar(3) not null,
    actual_departure dateTime,
    actual_arrival dateTime,
    update_ts dateTime
)");

            Console.WriteLine("FlightLoader");

            new FlightLoader().Load(lines, 1000);

            SQLExecutor.Run(@"create table frequent_flyer (
    frequent_flyer_id int not null,
    first_name varchar(16) not null,
    last_name varchar(16) not null,
    title varchar(16) not null,
    card_num varchar(16) not null,
    level int not null,
    award_points int not null,
    email varchar(32) not null,
    phone varchar(16) not null,
    update_ts dateTime
)");

            SQLExecutor.Run(@"create table passenger (
    passenger_id int not null,
    booking_id int not null,
    booking_ref varchar(32),
    passenger_no int,
    first_name varchar(16) not null,
    last_name varchar(16) not null,
    account_id int,
    update_ts dateTime,
    age int
)");

            SQLExecutor.Run(@"create table phone (
    phone_id int not null,
    account_id int,
    phone varchar(16),
    phone_type varchar(4),
    primary_phone int,
    update_ts dateTime
)");

            SQLExecutor.Run(@"create table boarding_pass (
    pass_id int not null,
    passenger_id int,
    booking_leg_id int,
    seat varchar(4),
    boarding_time dateTime,
    precheck int,
    update_ts dateTime
)");
        }

        private static IEnumerator<string> GetAllLines(string file)
        {
            return System.IO.File.ReadLines(file).GetEnumerator();
        }

        private static void SkipUntil(IEnumerator<string> lines, string expected)
        {
            while (lines.MoveNext())
            {
                var line = lines.Current;
                if (line == expected)
                    return;
            }
        }

        private static void ReadUntil(IEnumerator<string> lines, string expected, int groupBy, Action<List<string>> onLines)
        {
            List<string> buffer = new List<string>();
            
            while (lines.MoveNext())
            {
                var line = lines.Current;
                if (line == expected)
                {
                    if(buffer.Any())
                        onLines(buffer);
                    return;
                }

                buffer.Add(line);
                if (buffer.Count == groupBy)
                {
                    onLines(buffer);
                    buffer.Clear();
                }
            }
        }

        private static void ParseData(List<string> lines)
        {
            string sql = "insert into account (account_id, login, first_name, last_name, frequent_flyer_id, update_ts) values";

            foreach(var line in lines)
            {
                var values = line.Split('\t');

                sql += $"({values[0]}, '{values[1].Replace('\'', '_')}', '{values[2].Replace('\'', ' ')}', '{values[3].Replace('\'', ' ')}', {GetValOrNull(values[4])}, '{values[5]}'),";
            }

            ExecuteSQL(sql.TrimEnd(','));
        }

        private static string GetValOrNull(string v)
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

        private static void ExecuteSQL(string sql)
        {
            System.Console.WriteLine(sql);
            HttpClient client = new HttpClient();

            int attempt = 0;
            var response = client.PostAsync("http://localhost:5000/sql", new StringContent(sql)).Result;
            while(!response.IsSuccessStatusCode && attempt < 3)
            {
                attempt++;
                response = client.PostAsync("http://localhost:5000/sql", new StringContent(sql)).Result;
            }
        }
    }
}