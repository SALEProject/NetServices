using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using NetServices;

namespace TradeSessionScheduler
{
    class Program
    {

        public struct TSchedule
        {
            public string Code;
            public string Name;
            public string EntryPoint;
            public int ID_Market;
            public int ID;
            public int ID_Session;
            public int ID_Schedule;
            public string Status;
            public bool hasSchedule;
            public DateTime StartDate;
            public DateTime EndDate;
            public byte DaysOfWeek;
            public TimeSpan PreOpeningTime;
            public TimeSpan OpeningTime;
            public TimeSpan PreClosingTime;
            public TimeSpan ClosingTime;
            public string Visibility;
            public bool isElectronicSession;
            public bool launchAutomatically;

            public TSchedule(string Code, string Name, string EntryPoint, int ID_Market, int ID, int ID_Session, int ID_Schedule, string Status, bool hasSchedule, DateTime StartDate, DateTime EndDate, byte DaysOfWeek, TimeSpan PreOpeningTime, TimeSpan OpeningTime, TimeSpan PreClosingTime, TimeSpan ClosingTime, bool isElectronicSession = false, bool launchAutomatically = false)
            {
                this.Code = Code;
                this.Name = Name;
                this.EntryPoint = EntryPoint;
                this.ID_Market = ID_Market;
                this.ID = ID;
                this.ID_Session = ID_Session;
                this.ID_Schedule = ID_Schedule;
                this.Status = Status;
                this.hasSchedule = hasSchedule;
                this.StartDate = StartDate;
                this.EndDate = EndDate;
                this.DaysOfWeek = DaysOfWeek;
                this.PreOpeningTime = PreOpeningTime;
                this.OpeningTime = OpeningTime;
                this.PreClosingTime = PreClosingTime;
                this.ClosingTime = ClosingTime;
                this.Visibility = "";
                this.isElectronicSession = isElectronicSession;
                this.launchAutomatically = launchAutomatically;
            }
        }

        public struct TOrder
        {
            public int ID;
            public int ID_Market;
            public int ID_Ring;
            public int ID_Asset;
            public DateTime ExpirationDate;
        }

        //  short list of asset schedules to be checked
        public static System.Collections.ArrayList short_rings = new System.Collections.ArrayList();
        public static System.Collections.ArrayList short_assets = new System.Collections.ArrayList();
        public static System.Collections.ArrayList short_orders = new System.Collections.ArrayList();

        static void AddShortList(TSchedule ring_schedule, TSchedule asset_schedule)
        {
            short_rings.Add(ring_schedule);
            short_assets.Add(asset_schedule);
        }

        static void CheckShortList()
        {
            DBNow = GetDBNow();
            for (int i = 0; i < short_rings.Count; i++)
            {
                Console.Write(".");
                TSchedule ring_schedule = (TSchedule)short_rings[i];
                TSchedule asset_schedule = (TSchedule)short_assets[i];

                string status = asset_schedule.Status;
                updateAssetSession(ring_schedule, ref asset_schedule);
                if (status != asset_schedule.Status)
                {
                    short_rings.RemoveAt(i);
                    short_assets.RemoveAt(i);
                    i--;
                }
            }
        }

        static void AddShortOrders(TOrder order)
        {
            short_orders.Add(order);
        }

        static void CheckShortOrders()
        {
            DBNow = GetDBNow();
            for (int i = 0; i < short_orders.Count; i++)
            {
                Console.Write(".");
                TOrder order = (TOrder)short_orders[i];

                if (order.ExpirationDate <= DBNow)
                {
                    bitSimpleSQL.Execute("UPDATE [Orders] SET [isActive] = 0 WHERE [ID] = " + order.ID.ToString());
                    string str_Message_RO = "Ordinul " + order.ID.ToString() + " a fost dezactivat";
                    string str_Message_EN = "The order " + order.ID.ToString() + " has been deactivated";
                    InsertAlert(order.ID_Market, order.ID_Ring, order.ID_Asset, str_Message_RO, str_Message_RO, str_Message_EN);
                    Console.WriteLine(DateTime.Now.ToString() + ": " + str_Message_RO);
                    short_orders.RemoveAt(i);
                    i--;
                }
            }
        }

        //  utilitary methods  

        public static DateTime DBNow = DateTime.Now;

        static DateTime GetDBNow()
        {
            string sql = "SELECT GetDate() AS [Now]";
            DataSet ds_Now = bitSimpleSQL.Query(sql);
            if (ds_Now == null) { bitSimpleSQL.Disconnect(); Console.WriteLine(DateTime.Now.ToString() + ": DB disconnected"); return DateTime.Now; }
            if (ds_Now.Tables.Count == 0) return DateTime.Now;
            if (ds_Now.Tables[0].Rows.Count == 0) return DateTime.Now;

            return Convert.ToDateTime(ds_Now.Tables[0].Rows[0]["Now"]);
        }

        static void InsertAlert(int ID_Market, int ID_Ring, int ID_Asset, string Message, string Message_RO = "", string Message_EN = "")
        {
            if (Message_EN == "") Message_EN = Message;
            if (Message_RO == "") Message_RO = Message;
            bitSimpleSQL.Execute(@"INSERT INTO [Alerts] ([ID_Market], [ID_Ring], [ID_Asset], [Message], [Message_RO], [Message_EN]) 
                                   VALUES (" + ID_Market.ToString() + ", " + ID_Ring.ToString() + ", " + ID_Asset.ToString() + ", '" + Message + "', '" + Message_RO + "', '" + Message_EN + "')");
        }

        static void ResetRingSession(ref TSchedule ring_schedule)
        {
            string str_Message = "Ringul " + ring_schedule.Name + " a fost resetat";
            bitSimpleSQL.Execute("UPDATE [RingSessions] SET [Status] = 'NONE' WHERE [ID] = " + ring_schedule.ID_Session.ToString());
            //InsertAlert(ID_Market, ID_Ring, 0, str_Message);
            Console.WriteLine(DateTime.Now.ToString() + ": " + str_Message);
            ring_schedule.Status = "NONE";
        }

        static void PreopenRing(ref TSchedule ring_schedule)
        {
            string str_Message = "Ringul " + ring_schedule.Name + " este in deschidere";
            bitSimpleSQL.Execute("UPDATE [RingSessions] SET [Status] = 'PreOpened' WHERE [ID] = " + ring_schedule.ID_Session.ToString());
            //InsertAlert(ID_Market, ID_Ring, 0, str_Message);
            Console.WriteLine(DateTime.Now.ToString() + ": " + str_Message);
            ring_schedule.Status = "PreOpened";
        }

        static void OpenRing(ref TSchedule ring_schedule)
        {
            string str_Message = "Ringul " + ring_schedule.Name + " este deschis pentru tranzactii libere";
            bitSimpleSQL.Execute("UPDATE [RingSessions] SET [Status] = 'Opened' WHERE [ID] = " + ring_schedule.ID_Session.ToString());
            //InsertAlert(ring_schedule.ID_Market, ring_schedule.ID, 0, str_Message);
            Console.WriteLine(DateTime.Now.ToString() + ": " + str_Message);
            ring_schedule.Status = "Opened";
        }

        static void PrecloseRing(ref TSchedule ring_schedule)
        {
            string str_Message = "Ringul " + ring_schedule.Name + " este in curs de inchidere";
            bitSimpleSQL.Execute("UPDATE [RingSessions] SET [Status] = 'PreClosed' WHERE [ID] = " + ring_schedule.ID_Session.ToString());
            //InsertAlert(ID_Market, ID_Ring, 0, str_Message);
            Console.WriteLine(DateTime.Now.ToString() + ": " + str_Message);
            ring_schedule.Status = "PreClosed";
        }

        static void CloseRing(ref TSchedule ring_schedule)
        {
            string str_Message = "Ringul " + ring_schedule.Name + " este inchis";
            bitSimpleSQL.Execute("UPDATE [RingSessions] SET [Status] = 'Closed' WHERE [ID] = " + ring_schedule.ID_Session.ToString());
            //InsertAlert(ring_schedule.ID_Market, ring_schedule.ID, 0, str_Message);
            Console.WriteLine(DateTime.Now.ToString() + ": " + str_Message);
            ring_schedule.Status = "Closed";
        }

        static void ResetAssetSession(TSchedule ring_schedule, ref TSchedule asset_schedule)
        {
            string str_Message_RO = "Sedinta pentru activul " + asset_schedule.Name + '/' + ring_schedule.Name + " a fost resetata";
            string str_Message_EN = "The session for " + asset_schedule.Name + "/|" + ring_schedule.Name + " Asset has been reset";
            bitSimpleSQL.Execute(@"UPDATE [AssetSessions] SET [Status] = 'NONE', [PreOpeningTime] = NULL, [OpeningTime] = NULL, [PreClosingTime] = NULL, [ClosingTime] = NULL
                                   WHERE [ID] = " + asset_schedule.ID_Session.ToString());
            //bitSimpleSQL.Execute("UPDATE [Assets] SET [Code] = [Code] WHERE [ID] = " + asset_schedule.ID.ToString());
            InsertAlert(ring_schedule.ID_Market, ring_schedule.ID, asset_schedule.ID, str_Message_RO, str_Message_RO, str_Message_EN);
            Console.WriteLine(DateTime.Now.ToString() + ": " + str_Message_RO);
            asset_schedule.Status = "NONE";
        }

        static void PreopenAsset(TSchedule ring_schedule, ref TSchedule asset_schedule)
        {
            string str_Message_RO = "Sedinta pentru activul " + asset_schedule.Name + '/' + ring_schedule.Name + " este in deschidere";
            string str_Message_EN = "The session for " + asset_schedule.Name + '/' + ring_schedule.Name + " Asset in opening stage";
            bitSimpleSQL.Execute(@"UPDATE [AssetSessions] SET [Status] = 'PreOpened', 
                                   [PreOpeningTime] = '" + asset_schedule.PreOpeningTime.ToString() + @"', 
                                   [OpeningTime] = '" + asset_schedule.OpeningTime.ToString() + @"', 
                                   [PreClosingTime] = '" + asset_schedule.PreClosingTime.ToString() + @"', 
                                   [ClosingTime] = '" + asset_schedule.ClosingTime.ToString() + @"'
                                   WHERE [ID] = " + asset_schedule.ID_Session.ToString());

            //bitSimpleSQL.Execute("UPDATE [Assets] SET [Code] = [Code] WHERE [ID] = " + asset_schedule.ID.ToString());
            InsertAlert(ring_schedule.ID_Market, ring_schedule.ID, asset_schedule.ID, str_Message_RO, str_Message_RO, str_Message_EN);
            Console.WriteLine(DateTime.Now.ToString() + ": " + str_Message_RO);
            asset_schedule.Status = "PreOpened";
        }

        static void OpenAsset(TSchedule ring_schedule, ref TSchedule asset_schedule)
        {
            string str_Message_RO = "Activul " + asset_schedule.Name + '/' + ring_schedule.Name + " este disponibil pentru tranzactionare";
            string str_Message_EN = "Asset " + asset_schedule.Name + '/' + ring_schedule.Name + " is available for trading";
            bitSimpleSQL.Execute(@"UPDATE [AssetSessions] SET [Status] = 'Opened',
                                   [PreOpeningTime] = '" + asset_schedule.PreOpeningTime.ToString() + @"', 
                                   [OpeningTime] = '" + asset_schedule.OpeningTime.ToString() + @"', 
                                   [PreClosingTime] = '" + asset_schedule.PreClosingTime.ToString() + @"', 
                                   [ClosingTime] = '" + asset_schedule.ClosingTime.ToString() + @"'
                                   WHERE [ID] = " + asset_schedule.ID_Session.ToString());
            //bitSimpleSQL.Execute("UPDATE [Assets] SET [Code] = [Code] WHERE [ID] = " + asset_schedule.ID.ToString());
            InsertAlert(ring_schedule.ID_Market, ring_schedule.ID, asset_schedule.ID, str_Message_RO, str_Message_RO, str_Message_EN);
            Console.WriteLine(DateTime.Now.ToString() + ": " + str_Message_RO);
            asset_schedule.Status = "Opened";
        }

        static void PrecloseAsset(TSchedule ring_schedule, ref TSchedule asset_schedule)
        {
            string str_Message_RO = "Sedinta pentru activul " + asset_schedule.Name + '/' + ring_schedule.Name + " in curs de inchidere";
            string str_Message_EN = "The session for " + asset_schedule.Name + '/' + ring_schedule.Name + " Asset in closing stage";
            bitSimpleSQL.Execute(@"UPDATE [AssetSessions] SET [Status] = 'PreClosed',
                                   [PreOpeningTime] = '" + asset_schedule.PreOpeningTime.ToString() + @"', 
                                   [OpeningTime] = '" + asset_schedule.OpeningTime.ToString() + @"', 
                                   [PreClosingTime] = '" + asset_schedule.PreClosingTime.ToString() + @"', 
                                   [ClosingTime] = '" + asset_schedule.ClosingTime.ToString() + @"'
                                   WHERE [ID] = " + asset_schedule.ID_Session.ToString());
            //bitSimpleSQL.Execute("UPDATE [Assets] SET [Code] = [Code] WHERE [ID] = " + asset_schedule.ID.ToString());
            InsertAlert(ring_schedule.ID_Market, ring_schedule.ID, asset_schedule.ID, str_Message_RO, str_Message_RO, str_Message_EN);
            Console.WriteLine(DateTime.Now.ToString() + ": " + str_Message_RO);
            asset_schedule.Status = "PreClosed";
        }

        static void CloseAsset(TSchedule ring_schedule, ref TSchedule asset_schedule)
        {
            string str_Message_RO = "Sedinta pentru activul " + asset_schedule.Name + '/' + ring_schedule.Name + " este inchisa";
            string str_Message_EN = "The session for " + asset_schedule.Name + '/' + ring_schedule.Name + " Asset is closed";
            bitSimpleSQL.Execute(@"UPDATE [AssetSessions] SET [Status] = 'Closed',
                                   [PreOpeningTime] = '" + asset_schedule.PreOpeningTime.ToString() + @"', 
                                   [OpeningTime] = '" + asset_schedule.OpeningTime.ToString() + @"', 
                                   [PreClosingTime] = '" + asset_schedule.PreClosingTime.ToString() + @"', 
                                   [ClosingTime] = '" + asset_schedule.ClosingTime.ToString() + @"'
                                   WHERE [ID] = " + asset_schedule.ID_Session.ToString());
            //bitSimpleSQL.Execute("UPDATE [Assets] SET [Code] = [Code] WHERE [ID] = " + asset_schedule.ID.ToString());
            InsertAlert(ring_schedule.ID_Market, ring_schedule.ID, asset_schedule.ID, str_Message_RO, str_Message_RO, str_Message_EN);
            Console.WriteLine(DateTime.Now.ToString() + ": " + str_Message_RO);
            asset_schedule.Status = "Closed";

            ClearOrderMatches(asset_schedule.ID);
        }

        static int GetRingSession(int ID_Ring, bool CreateSession = false)
        {
            string sql = "SELECT * FROM [RingSessions] WHERE ([ID_Ring] = " + ID_Ring.ToString() + ") AND (CONVERT(VARCHAR(20), [Date], 102) = CONVERT(VARCHAR(20), CAST('" + (DBNow.Year * 10000 + DBNow.Month * 100 + DBNow.Day).ToString() + "' AS DATETIME), 102))";
            //string sql = "SELECT TOP 1 * FROM [RingSessions] WHERE [ID_Ring] = " + ID_Ring.ToString() + " ORDER BY [Date] DESC";
            DataSet ds_session = bitSimpleSQL.Query(sql);
            if (ds_session == null) return 0; //  sql or db error
            if (ds_session.Tables.Count == 0) return 0; //  sql or db error

            if (ds_session.Tables[0].Rows.Count > 0 /*&& Convert.ToDateTime(ds_session.Tables[0].Rows[0]["Date"]).Date == DateTime.Today*/)
                return Convert.ToInt32(ds_session.Tables[0].Rows[0]["ID"]);

            //  create a new session
            sql = "INSERT INTO [RingSessions] ([ID_Ring]) VALUES (" + ID_Ring.ToString() + ")";
            bitSimpleSQL.Execute(sql);

            sql = "SELECT * FROM [RingSessions] WHERE ([ID_Ring] = " + ID_Ring.ToString() + ") AND (CONVERT(VARCHAR(20), [Date], 102) = CONVERT(VARCHAR(20), CAST('" + (DBNow.Year * 10000 + DBNow.Month * 100 + DBNow.Day).ToString() + "' AS DATETIME), 102))";
            ds_session = bitSimpleSQL.Query(sql);
            if (ds_session == null) return 0; //  sql or db error
            if (ds_session.Tables.Count == 0) return 0; //  sql or db error
            if (ds_session.Tables[0].Rows.Count > 0)
                return Convert.ToInt32(ds_session.Tables[0].Rows[0]["ID"]);

            return 0;
        }

        static TSchedule getRingSchedule(int ID_Ring, int ID_RingSession = 0)
        {
            TSchedule res = new TSchedule("", "", "", 0, 0, 0, 0, "", false, DateTime.Today, DateTime.Today, 0, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero);

            string sql = "SELECT B.[Code] AS [EntryPoint], R.* FROM [Rings] R LEFT JOIN [Markets] M ON (R.[ID_Market] = M.[ID]) LEFT JOIN [Bursaries] B on (M.[ID_Bursary] = B.[ID]) WHERE R.[ID] = " + ID_Ring.ToString();
            DataSet ds_ring = bitSimpleSQL.Query(sql);
            if (ds_ring == null) return res;
            if (ds_ring.Tables.Count == 0) return res;
            if (ds_ring.Tables[0].Rows.Count == 0) return res;

            res.Code = ds_ring.Tables[0].Rows[0]["Code"].ToString();
            res.Name = ds_ring.Tables[0].Rows[0]["Name"].ToString();
            res.EntryPoint = ds_ring.Tables[0].Rows[0]["EntryPoint"].ToString();
            res.ID_Market = Convert.ToInt32(ds_ring.Tables[0].Rows[0]["ID_Market"]);
            res.ID = ID_Ring;
            res.ID_Session = ID_RingSession;
            res.hasSchedule = Convert.ToBoolean(ds_ring.Tables[0].Rows[0]["hasSchedule"]);
            if (res.hasSchedule)
            {
                res.StartDate = DateTime.Today;
                res.EndDate = DateTime.Today;
                if (ds_ring.Tables[0].Rows[0]["StartDate"] != DBNull.Value) res.StartDate = Convert.ToDateTime(ds_ring.Tables[0].Rows[0]["StartDate"]);
                if (ds_ring.Tables[0].Rows[0]["EndDate"] != DBNull.Value) res.EndDate = Convert.ToDateTime(ds_ring.Tables[0].Rows[0]["EndDate"]);

                res.DaysOfWeek = Convert.ToByte(ds_ring.Tables[0].Rows[0]["DaysOfWeek"]);
                res.PreOpeningTime = (TimeSpan)ds_ring.Tables[0].Rows[0]["PreOpeningTime"];
                res.OpeningTime = (TimeSpan)ds_ring.Tables[0].Rows[0]["OpeningTime"];
                res.PreClosingTime = (TimeSpan)ds_ring.Tables[0].Rows[0]["PreClosingTime"];
                res.ClosingTime = (TimeSpan)ds_ring.Tables[0].Rows[0]["ClosingTime"];

                res.isElectronicSession = true;
                res.launchAutomatically = true;
            }
            else
            {
                res.StartDate = DBNow.Date;
                res.EndDate = DBNow.Date;
                res.DaysOfWeek = 127;
                res.PreOpeningTime = TimeSpan.Zero;
                res.OpeningTime = TimeSpan.Zero;
                res.PreClosingTime = TimeSpan.FromHours(24);
                res.ClosingTime = TimeSpan.FromHours(24);
            }

            if (ID_RingSession != 0)
            {
                sql = "SELECT * FROM [RingSessions] WHERE ([ID_Ring] = " + ID_Ring.ToString() + ") AND ([ID] = " + ID_RingSession.ToString() + ")";
                DataSet ds_session = bitSimpleSQL.Query(sql);
                if (ds_session == null) return res;
                if (ds_session.Tables.Count == 0) return res;
                if (ds_session.Tables[0].Rows.Count == 0) return res;

                res.Status = ds_session.Tables[0].Rows[0]["Status"].ToString();
            }

            if (res.Name.IndexOf("fld_Ring_Name") >= 0)
            {
                sql = "SELECT * FROM [Translations] WHERE [Label] = '" + res.Name + "'";
                DataSet ds_translation = bitSimpleSQL.Query(sql);
                if (ds_translation == null) return res;
                if (ds_translation.Tables.Count == 0) return res;
                if (ds_translation.Tables[0].Rows.Count == 0) return res;

                res.Name = ds_translation.Tables[0].Rows[0]["Value_RO"].ToString();
            }

            return res;
        }

        static void updateRingSession(ref TSchedule ringSchedule)
        {
            string Status = ringSchedule.Status;

            byte CheckDay = 0;
            switch (DateTime.Today.DayOfWeek)
            {
                case DayOfWeek.Sunday:
                    CheckDay = 1;                 
                    break;
                case DayOfWeek.Monday:
                    CheckDay = 2;
                    break;
                case DayOfWeek.Tuesday:
                    CheckDay = 4;
                    break;
                case DayOfWeek.Wednesday:
                    CheckDay = 8;
                    break;
                case DayOfWeek.Thursday:
                    CheckDay = 16;
                    break;
                case DayOfWeek.Friday:
                    CheckDay = 32;
                    break;
                case DayOfWeek.Saturday:
                    CheckDay = 64;
                    break;
            }

            if (ringSchedule.hasSchedule && (ringSchedule.DaysOfWeek & CheckDay) == 0)
            {
                if (Status != "NONE") ResetRingSession(ref ringSchedule);
            }
            else
            {

                if (DBNow.TimeOfDay < ringSchedule.PreOpeningTime && Status != "NONE")
                {
                    ResetRingSession(ref ringSchedule);
                }

                if (DBNow.TimeOfDay >= ringSchedule.PreOpeningTime && DBNow.TimeOfDay <= ringSchedule.OpeningTime && Status != "PreOpened")
                {
                    PreopenRing(ref ringSchedule);
                }

                if (DBNow.TimeOfDay >= ringSchedule.OpeningTime && DBNow.TimeOfDay <= ringSchedule.PreClosingTime && Status != "Opened")
                {
                    OpenRing(ref ringSchedule);
                }

                if (DBNow.TimeOfDay >= ringSchedule.PreClosingTime && DBNow.TimeOfDay <= ringSchedule.ClosingTime && Status != "PreClosed")
                {
                    PrecloseRing(ref ringSchedule);
                }

                if (DBNow.TimeOfDay >= ringSchedule.ClosingTime && Status != "Closed")
                {
                    CloseRing(ref ringSchedule);
                }
            }
        }

        static int GetAssetSession(int ID_Ring, int ID_Asset, ref TSchedule assetSchedule)
        {
            string sql = "SELECT * FROM [AssetSessions] WHERE ([ID_Ring] = " + ID_Ring.ToString() + ") AND ([ID_Asset] = " + ID_Asset.ToString() + ") AND (CONVERT(VARCHAR(20), [Date], 102) = CONVERT(VARCHAR(20), CAST('" + (DBNow.Year * 10000 + DBNow.Month * 100 + DBNow.Day).ToString() + "' AS DATETIME), 102))";
            DataSet ds_session = bitSimpleSQL.Query(sql);
            if (ds_session == null) return 0;
            if (ds_session.Tables.Count == 0) return 0;

            if (ds_session.Tables[0].Rows.Count > 0)
            {
                assetSchedule.ID_Session = Convert.ToInt32(ds_session.Tables[0].Rows[0]["ID"]);
                assetSchedule.Status = ds_session.Tables[0].Rows[0]["Status"].ToString();
                return assetSchedule.ID_Session;
            }

            //  create new session for the asset
            if (assetSchedule.Visibility == "public" && assetSchedule.isElectronicSession && assetSchedule.launchAutomatically)
            {
                bitSimpleSQL.Execute("INSERT INTO [AssetSessions] ([ID_Ring], [ID_Asset], [ID_AssetSchedule]) VALUES (" + ID_Ring.ToString() + ", " + ID_Asset.ToString() + ", " + assetSchedule.ID_Schedule + ")");

                sql = "SELECT * FROM [AssetSessions] WHERE ([ID_Ring] = " + ID_Ring.ToString() + ") AND ([ID_Asset] = " + ID_Asset.ToString() + ") AND (CONVERT(VARCHAR(20), [Date], 102) = CONVERT(VARCHAR(20), CAST('" + (DBNow.Year * 10000 + DBNow.Month * 100 + DBNow.Day).ToString() + "' AS DATETIME), 102))";
                ds_session = bitSimpleSQL.Query(sql);
                if (ds_session == null) return 0;
                if (ds_session.Tables.Count == 0) return 0;

                if (ds_session.Tables[0].Rows.Count > 0)
                {
                    assetSchedule.ID_Session = Convert.ToInt32(ds_session.Tables[0].Rows[0]["ID"]);
                    assetSchedule.Status = "NONE";
                    return assetSchedule.ID_Session;
                }
            }

            return 0;
        }

        static TSchedule GetAssetSchedule(int ID_Asset, TSchedule ringSchedule)
        {
            TSchedule res = new TSchedule("", "", "", 0, 0, 0, 0, "", false, DateTime.Today, DateTime.Today, 0, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero);
            res.EntryPoint = ringSchedule.EntryPoint;
            res.ID_Market = ringSchedule.ID_Market;
            res.Status = "NONE";
            res.StartDate = ringSchedule.StartDate;
            res.EndDate = ringSchedule.EndDate;
            res.DaysOfWeek = ringSchedule.DaysOfWeek;
            res.PreOpeningTime = ringSchedule.PreOpeningTime;
            res.OpeningTime = ringSchedule.OpeningTime;
            res.PreClosingTime = ringSchedule.PreClosingTime;
            res.ClosingTime = ringSchedule.ClosingTime;
            res.isElectronicSession = ringSchedule.isElectronicSession;
            res.launchAutomatically = ringSchedule.launchAutomatically;

            bool Inherit = false;
            string sql = "SELECT [inheritRingSchedule], [Visibility] FROM [Assets] WHERE [ID] = " + ID_Asset.ToString();
            DataSet ds_asset = bitSimpleSQL.Query(sql);
            if (ds_asset != null)
                if (ds_asset.Tables.Count > 0)
                    if (ds_asset.Tables[0].Rows.Count > 0)
                    {
                        Inherit = Convert.ToBoolean(ds_asset.Tables[0].Rows[0]["inheritRingSchedule"]);
                        res.Visibility = ds_asset.Tables[0].Rows[0]["Visibility"].ToString();
                    }

            sql = "SELECT * FROM [AssetSchedules] WHERE [ID_Asset] = " + ID_Asset.ToString();
            DataSet ds_schedules = bitSimpleSQL.Query(sql);
            if (ds_schedules == null) return res;
            if (ds_schedules.Tables.Count == 0) return res;
            if (ds_schedules.Tables[0].Rows.Count == 0 && Inherit)
            {
                res.ID = ID_Asset;
                return res;
            }

            foreach (DataRow row in ds_schedules.Tables[0].Rows)
            {
                //  check date interval
                if (Convert.ToDateTime(row["StartDate"]) > DateTime.Today || Convert.ToDateTime(row["EndDate"]) < DateTime.Today) continue;

                //  check day of week
                byte DaysOfWeek = Convert.ToByte(row["DaysOfWeek"]);
                switch (DateTime.Today.DayOfWeek)
                {
                    case DayOfWeek.Sunday:
                        if ((DaysOfWeek & 1) == 0) continue;
                        break;
                    case DayOfWeek.Monday:
                        if ((DaysOfWeek & 2) == 0) continue;
                        break;
                    case DayOfWeek.Tuesday:
                        if ((DaysOfWeek & 4) == 0) continue;
                        break;
                    case DayOfWeek.Wednesday:
                        if ((DaysOfWeek & 8) == 0) continue;
                        break;
                    case DayOfWeek.Thursday:
                        if ((DaysOfWeek & 16) == 0) continue;
                        break;
                    case DayOfWeek.Friday:
                        if ((DaysOfWeek & 32) == 0) continue;
                        break;
                    case DayOfWeek.Saturday:
                        if ((DaysOfWeek & 64) == 0) continue;
                        break;
                }

                //  if future criteria are necessary add them here

                //  extract and return schedule
                res.ID = ID_Asset;
                res.ID_Schedule = Convert.ToInt32(row["ID"]);
                res.Status = "NONE";
                res.StartDate = Convert.ToDateTime(row["StartDate"]);
                res.EndDate = Convert.ToDateTime(row["EndDate"]);
                res.DaysOfWeek = Convert.ToByte(row["DaysOfWeek"]);
                res.PreOpeningTime = (TimeSpan)row["PreOpeningTime"];
                res.OpeningTime = (TimeSpan)row["OpeningTime"];
                res.PreClosingTime = (TimeSpan)row["PreClosingTime"];
                res.ClosingTime = (TimeSpan)row["ClosingTime"];
                res.isElectronicSession = Convert.ToBoolean(row["isElectronicSession"]);
                res.launchAutomatically = Convert.ToBoolean(row["launchAutomatically"]);

                //  not really kusher
                if (res.PreOpeningTime > TimeSpan.Zero)
                    res.PreOpeningTime.Subtract(new TimeSpan(0, 0, 1));

                return res;
            }

            return res;
        }

        static bool updateAssetSession(TSchedule ringSchedule, ref TSchedule assetSchedule)
        {
            string Status = assetSchedule.Status;

            if (assetSchedule.Visibility != "public" || !assetSchedule.isElectronicSession || !assetSchedule.launchAutomatically)
            {
                if (Status != "NONE")
                {
                    ResetAssetSession(ringSchedule, ref assetSchedule);
                    if (assetSchedule.EntryPoint == "BTGN") InvalidateOrders(ringSchedule, assetSchedule);
                }
            }
            else
            {
                if (DBNow.TimeOfDay < assetSchedule.PreOpeningTime && Status != "NONE")
                {
                    ResetAssetSession(ringSchedule, ref assetSchedule);
                    if (assetSchedule.EntryPoint == "BTGN") InvalidateOrders(ringSchedule, assetSchedule);
                }

                if (DBNow.TimeOfDay >= assetSchedule.PreOpeningTime && DBNow.TimeOfDay <= assetSchedule.OpeningTime && Status != "PreOpened")
                {
                    PreopenAsset(ringSchedule, ref assetSchedule);
                    if (assetSchedule.EntryPoint == "BTGN") InvalidateOrders(ringSchedule, assetSchedule);
                }

                if (DBNow.TimeOfDay >= assetSchedule.OpeningTime && DBNow.TimeOfDay <= assetSchedule.PreClosingTime && Status != "Opened")
                {
                    OpenAsset(ringSchedule, ref assetSchedule);
                    if (assetSchedule.EntryPoint == "BTGN") InvalidateOrders(ringSchedule, assetSchedule);
                    ResetDeltas(ringSchedule, assetSchedule);
                }

                if (DBNow.TimeOfDay >= assetSchedule.PreClosingTime && DBNow.TimeOfDay <= assetSchedule.ClosingTime && Status != "PreClosed")
                {
                    PrecloseAsset(ringSchedule, ref assetSchedule);
                }

                if (DBNow.TimeOfDay >= assetSchedule.ClosingTime && Status != "Closed")
                {
                    CloseAsset(ringSchedule, ref assetSchedule);
                }

                if (Status == assetSchedule.Status)
                {
                    TimeSpan delta = new TimeSpan(0, 0, 10);
                    TimeSpan dt0 = assetSchedule.PreOpeningTime.Subtract(DBNow.TimeOfDay);
                    TimeSpan dt1 = assetSchedule.OpeningTime.Subtract(DBNow.TimeOfDay);
                    TimeSpan dt2 = assetSchedule.PreClosingTime.Subtract(DBNow.TimeOfDay);
                    TimeSpan dt3 = assetSchedule.ClosingTime.Subtract(DBNow.TimeOfDay);

                    if (Status == "NONE" && dt0 > TimeSpan.Zero && dt0 < delta) return true;
                    if (Status == "PreOpened" && dt1 > TimeSpan.Zero && dt1 < delta) return true;
                    if (Status == "Opened" && dt2 > TimeSpan.Zero && dt2 < delta) return true;
                    if (Status == "PreClosed" && dt3 > TimeSpan.Zero && dt3 < delta) return true;
                }
            }

            return false;
        }

        static void ClosePreviousSessions()
        {
            bitSimpleSQL.Execute("UPDATE [RingSessions] SET [Status] = 'Closed' WHERE (CONVERT(VARCHAR, [Date], 102) < CONVERT(VARCHAR, GetDate(), 102)) AND ([Status] != 'Closed')");
            bitSimpleSQL.Execute("UPDATE [AssetSessions] SET [Status] = 'Closed' WHERE (CONVERT(VARCHAR, [Date], 102) < CONVERT(VARCHAR, GetDate(), 102)) AND ([Status] != 'Closed')");
        }

        static void ClearOrderMatches(int ID_Asset)
        {
            string sql = @"DELETE FROM [OrderMatchDetails] WHERE [ID_Order] IN
                           (
                             SELECT [ID] FROM [Orders] WHERE [ID_Asset] = " + ID_Asset.ToString() + @"
                           )
      
                           DELETE FROM [OrderMatches] WHERE [ID_Order] = " + ID_Asset.ToString();
            
            bitSimpleSQL.Execute(sql);
        }

        static void InvalidateOrders(TSchedule ringSchedule, TSchedule assetSchedule)
        {
            //  select parameters
            DataSet ds_rings = bitSimpleSQL.Query("SELECT * FROM [Rings] WHERE [ID] = " + ringSchedule.ID.ToString());
            if (ds_rings == null) return;
            if (ds_rings.Tables.Count == 0) return;
            if (ds_rings.Tables[0].Rows.Count == 0) return;

            DataSet ds_parameters = bitSimpleSQL.Query("SELECT * FROM [RingParameters_RGN] WHERE [ID_Ring] = " + ringSchedule.ID.ToString());
            if (ds_parameters == null) return;
            if (ds_parameters.Tables.Count == 0) return;
            if (ds_parameters.Tables[0].Rows.Count == 0) return;

            int int_StartDeliveryDateOffset = Convert.ToInt32(ds_parameters.Tables[0].Rows[0]["StartDeliveryDateOffset"]);
            int int_EndDeliveryDateOffset = Convert.ToInt32(ds_parameters.Tables[0].Rows[0]["EndDeliveryDateOffset"]);

            //  iterate through all active orders
            DataSet ds_orders = bitSimpleSQL.Query("SELECT * FROM [Orders] O LEFT JOIN [OrderDetails_RGN] RGN ON (O.[ID] = RGN.[ID_Order]) WHERE (O.[ID_Ring] = " + ringSchedule.ID.ToString() + ") AND (O.[isActive] = 1)");
            if (ds_orders == null) return;
            if (ds_orders.Tables.Count == 0) return;
            for (int i = 0; i < ds_orders.Tables[0].Rows.Count; i++)
            {
                try
                {
                    int ID_Order = Convert.ToInt32(ds_orders.Tables[0].Rows[i]["ID_Order"]);
                    int ID_Asset = Convert.ToInt32(ds_orders.Tables[0].Rows[i]["ID_Asset"]);
                    DateTime dt_StartDeliveryDate = Convert.ToDateTime(ds_orders.Tables[0].Rows[i]["StartDeliveryDate"]);
                    DateTime dt_EndDeliveryDate = Convert.ToDateTime(ds_orders.Tables[0].Rows[i]["EndDeliveryDate"]);

                    int ord_ID_RingSession = Convert.ToInt32(ds_orders.Tables[0].Rows[i]["ID_RingSession"]);
                    int ord_ID_AssetSession = Convert.ToInt32(ds_orders.Tables[0].Rows[i]["ID_AssetSession"]);

                    bool b = false;
                    if (dt_StartDeliveryDate < DateTime.Today.AddDays(int_StartDeliveryDateOffset)) b = true;
                    if (dt_EndDeliveryDate > DateTime.Today.AddDays(int_EndDeliveryDateOffset)) b = true;
                    if (dt_EndDeliveryDate < DateTime.Today.AddDays(int_StartDeliveryDateOffset)) b = true;
                    if (dt_EndDeliveryDate < dt_StartDeliveryDate) b = true;

                    if (b)
                    {
                        bitSimpleSQL.Execute("UPDATE [Orders] SET [isActive] = 0 WHERE [ID] = " + ID_Order.ToString());
                        string str_Message_RO = "Ordinul " + ID_Order.ToString() + " a fost dezactivat";
                        string str_Message_EN = "The order " + ID_Order.ToString() + " has been deactivated";
                        InsertAlert(ringSchedule.ID_Market, ringSchedule.ID, ID_Asset, str_Message_RO, str_Message_RO, str_Message_EN);
                        Console.WriteLine(DateTime.Now.ToString() + ": " + str_Message_RO);
                    }
                    else
                    {
                        int ID_RingSession = ringSchedule.ID_Session;
                        int ID_AssetSession = assetSchedule.ID_Session;
                        if (ID_RingSession <= 0 || ID_AssetSession <= 0) continue;

                        if (ord_ID_RingSession != ID_RingSession && ord_ID_AssetSession != ID_AssetSession)
                            bitSimpleSQL.Execute("UPDATE [Orders] SET [ID_RingSession] = " + ID_RingSession.ToString() + ", [ID_AssetSession] = " + ID_AssetSession.ToString() + " WHERE [ID] = " + ID_Order.ToString());
                    }
                }
                catch { }
            }
        }


        static void checkOrderExpiration(TSchedule ringSchedule, TSchedule assetSchedule)
        {
            //  iterate through all active orders
            string sql = "SELECT * FROM [Orders] O WHERE (O.[ID_Ring] = " + ringSchedule.ID.ToString() + ") AND (O.[ID_Asset] = " + assetSchedule.ID + ") AND (O.[isActive] = 1)";
            DataSet ds_orders = bitSimpleSQL.Query(sql);
            if (ds_orders == null) return;
            if (ds_orders.Tables.Count == 0) return;
            foreach (DataRow row in ds_orders.Tables[0].Rows)
            {
                try
                {
                    int ID_Order = Convert.ToInt32(row["ID"]);
                    int ID_Asset = Convert.ToInt32(row["ID_Asset"]);

                    int ord_ID_RingSession = Convert.ToInt32(row["ID_RingSession"]);
                    int ord_ID_AssetSession = Convert.ToInt32(row["ID_AssetSession"]);

                    DateTime ExpirationDate = Convert.ToDateTime(row["ExpirationDate"]);
                    TOrder order = new TOrder() { ID = ID_Order, ID_Market = ringSchedule.ID_Market, ID_Ring = ringSchedule.ID, ID_Asset = ID_Asset, ExpirationDate = ExpirationDate };

                    //  check all conditions to invalidate
                    bool doInvalidate = false;
                    if (DBNow > ExpirationDate) doInvalidate = true;
                    else if (DBNow.AddSeconds(10) > ExpirationDate) AddShortOrders(order);

                    if (doInvalidate)
                    {
                        bitSimpleSQL.Execute("UPDATE [Orders] SET [isActive] = 0 WHERE [ID] = " + ID_Order.ToString());
                        string str_Message_RO = "Ordinul " + ID_Order.ToString() + " a fost dezactivat";
                        string str_Message_EN = "The order " + ID_Order.ToString() + " has been deactivated";
                        InsertAlert(ringSchedule.ID_Market, ringSchedule.ID, ID_Asset, str_Message_RO, str_Message_RO, str_Message_EN);
                        Console.WriteLine(DateTime.Now.ToString() + ": " + str_Message_RO);
                    }
                    else
                    {
                        int ID_RingSession = ringSchedule.ID_Session;
                        int ID_AssetSession = assetSchedule.ID_Session;
                        if (ID_RingSession <= 0 || ID_AssetSession <= 0) continue;

                        if (ord_ID_RingSession != ID_RingSession || ord_ID_AssetSession != ID_AssetSession)
                            bitSimpleSQL.Execute("UPDATE [Orders] SET [ID_RingSession] = " + ID_RingSession.ToString() + ", [ID_AssetSession] = " + ID_AssetSession.ToString() + " WHERE [ID] = " + ID_Order.ToString());
                    }
                }
                catch { }
            }

            //  check for orphan order matches
            bitSimpleSQL.Execute("DELETE FROM [OrderMatchDetails] WHERE [ID] IN ( SELECT OMD.[ID] FROM [OrderMatchDetails] OMD LEFT JOIN [Orders] O ON (OMD.[ID_Order] = O.[ID]) WHERE O.[isActive] = 0 )");
            bitSimpleSQL.Execute("DELETE FROM [OrderMatches] WHERE [ID] IN ( SELECT OM.[ID] FROM [OrderMatches] OM LEFT JOIN [Orders] O ON (OM.[ID_Order] = O.[ID]) WHERE O.[isActive] = 0 )");
        }

        static void ResetDeltas(TSchedule ringSchedule, TSchedule assetSchedule)
        {
            //  iterate through all active orders
            string sql = "SELECT * FROM [Orders] O WHERE (O.[ID_Ring] = " + ringSchedule.ID.ToString() + ") AND (O.[ID_Asset] = " + assetSchedule.ID + ") AND (O.[isActive] = 1)";
            DataSet ds_orders = bitSimpleSQL.Query(sql);
            if (ds_orders == null) return;
            if (ds_orders.Tables.Count == 0) return;
            foreach (DataRow row in ds_orders.Tables[0].Rows)
            {
                try
                {
                    int ID_Order = Convert.ToInt32(row["ID"]);
                    int ID_Asset = Convert.ToInt32(row["ID_Asset"]);

                    bitSimpleSQL.Execute("INSERT INTO [Events] ([Priority], [Resource], [EventType], [ID_Resource], [ID_LinkedResource]) VALUES (0, 'DeltaT', 'reset', " + ID_Asset.ToString() + ", " + ID_Order.ToString() + ")");
                }
                catch { }
            }
        }

        static void Main(string[] args)
        {
            bitSimpleSQL.Connect("DB.xml");

            Random rnd = new Random(DateTime.Now.Millisecond);
            string sql = "";
            int counter = 0;

            while (!Console.KeyAvailable)
            {
                if (counter % 600000 == 0)
                {
                    counter = 0;
                    bitSimpleSQL.Disconnect();
                    if (!bitSimpleSQL.Connect("DB.xml")) Console.WriteLine(DateTime.Now.ToString() + ": DB reconnect error");
                    else Console.WriteLine(DateTime.Now.ToString() + ": DB reconnected");
                }

                if (counter % 6000 == 0 && !bitSimpleSQL.Connected)
                {
                    Console.WriteLine(DateTime.Now.ToString() + ": reconnecting...");
                    if (!bitSimpleSQL.Connect("DB.xml")) Console.WriteLine(DateTime.Now.ToString() + ": DB reconnect error");
                    else Console.WriteLine(DateTime.Now.ToString() + ": DB reconnected");
                }

                if (bitSimpleSQL.Connected)
                {
                    if (counter % 200 == 0) CheckShortList();
                    if (counter % 500 == 0) CheckShortOrders();

                    if (counter % 60000 == 0)
                    {
                        DBNow = GetDBNow();
                        ClosePreviousSessions();
                    }

                    if (counter % 10000 == 0)
                    {

                        DBNow = GetDBNow();

                        try
                        {
                            sql = "SELECT B.[Code] AS [BursaryCode], R.* FROM [Rings] R LEFT JOIN [Markets] M ON (R.[ID_Market] = M.[ID]) LEFT JOIN [Bursaries] B ON (M.[ID_Bursary] = B.[ID]) WHERE R.[isActive] = 1";
                            DataSet ds_rings = bitSimpleSQL.Query(sql);
                            if (ds_rings == null) continue;
                            if (ds_rings.Tables.Count == 0) continue;
                            foreach (DataRow row_ring in ds_rings.Tables[0].Rows)
                            {
                                string BursaryCode = row_ring["BursaryCode"].ToString();
                                int ID_Ring = Convert.ToInt32(row_ring["ID"]);
                                int ID_Market = Convert.ToInt32(row_ring["ID_Market"]);

                                int ID_RingSession = GetRingSession(ID_Ring, true);
                                if (ID_RingSession == 0) continue;
                                TSchedule ring_schedule = getRingSchedule(ID_Ring, ID_RingSession);
                                updateRingSession(ref ring_schedule);

                                //  check all assets that inherit the Ring schedule or have a customized schedule
                                sql = "SELECT DISTINCT A.*, " +
                                      "(SELECT [ID] FROM [AssetSessions] WHERE ([ID_Asset] = A.[ID]) AND (CONVERT(DATE, [Date]) = CONVERT(DATE, GetDate())))  AS [ID_AssetSession], " +
                                      "(SELECT [Status] FROM [AssetSessions] WHERE ([ID_Asset] = A.[ID]) AND (CONVERT(DATE, [Date]) = CONVERT(DATE, GetDate()))) AS [Status] " +
                                      "FROM [Assets] A " +
                                      "LEFT JOIN [AssetSchedules] ASCH ON (A.[ID] = ASCH.[ID_Asset]) " +
                                      "LEFT JOIN [AssetSessions] ASS ON (A.[ID] = ASS.[ID_Asset]) " +
                                      "WHERE (A.[ID_Ring] = " + ID_Ring.ToString() + ") AND (A.[isActive] = 1) AND (A.[isDeleted] = 0) " +
                                      "AND ((A.[inheritRingSchedule] = 1) OR (GetDate() BETWEEN ASCH.[StartDate] AND DATEADD(day, 1, ASCH.[EndDate])) " +
                                      "OR (CONVERT(DATE, ASS.[Date]) = CONVERT(DATE, GetDate())))";
                                DataSet ds_assets = bitSimpleSQL.Query(sql);
                                if (ds_assets == null) continue;
                                if (ds_assets.Tables.Count == 0) continue;
                                foreach (DataRow row_asset in ds_assets.Tables[0].Rows)
                                {
                                    int ID_Asset = Convert.ToInt32(row_asset["ID"]);

                                    TSchedule asset_schedule = GetAssetSchedule(ID_Asset, ring_schedule);
                                    if (asset_schedule.ID == 0)
                                    {
                                        if (row_asset["ID_AssetSession"] != DBNull.Value)
                                        {
                                            asset_schedule.ID = ID_Asset;
                                            asset_schedule.ID_Session = Convert.ToInt32(row_asset["ID_AssetSession"]);
                                            asset_schedule.Status = row_asset["Status"].ToString();
                                            updateAssetSession(ring_schedule, ref asset_schedule);
                                        }
                                    }
                                    else
                                    {
                                        asset_schedule.Code = row_asset["Code"].ToString();
                                        asset_schedule.Name = row_asset["Name"].ToString();

                                        if (asset_schedule.Name.IndexOf("fld_Asset_Name") >= 0)
                                        {
                                            sql = "SELECT * FROM [Translations] WHERE [Label] = '" + asset_schedule.Name + "'";
                                            DataSet ds_translation = bitSimpleSQL.Query(sql);
                                            if (ds_translation != null)
                                                if (ds_translation.Tables.Count > 0)
                                                    if (ds_translation.Tables[0].Rows.Count > 0) asset_schedule.Name = ds_translation.Tables[0].Rows[0]["Value_RO"].ToString();
                                        }

                                        int ID_AssetSession = GetAssetSession(ID_Ring, ID_Asset, ref asset_schedule);
                                        if (ID_AssetSession != 0)
                                        {
                                            bool b_short = updateAssetSession(ring_schedule, ref asset_schedule);
                                            if (b_short)
                                                AddShortList(ring_schedule, asset_schedule);
                                        }
                                    }

                                    asset_schedule.ID = ID_Asset;
                                    if (BursaryCode == "DISPONIBIL") checkOrderExpiration(ring_schedule, asset_schedule);

                                }
                            }

                        }
                        catch (Exception exc)
                        {
                            Console.WriteLine(DateTime.Now.ToString() + ": Error: " + exc.Message);
                        }
                    }
                }

                System.Threading.Thread.Sleep(100);
                counter += 100;
                if (counter > 300000) counter = 0;
            }

            bitSimpleSQL.Disconnect();
        }
    }
}
