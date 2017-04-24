using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using NetServices;

namespace MarketDataGenerator
{
    class Program
    {
        static Random rnd = new Random(DateTime.Now.Millisecond);

        static void Calculate5minRND(TimeSpan currentTick, ref double OpenPrice, ref double MinPrice, ref double MaxPrice, ref double ClosePrice)
        {
            if (OpenPrice == 0) OpenPrice = 110;
            ClosePrice = OpenPrice - 10 + 20 * rnd.NextDouble();

            MinPrice = Math.Min(OpenPrice, ClosePrice);
            MaxPrice = Math.Max(OpenPrice, ClosePrice);

            double flt1 = OpenPrice - 10 + 30 * rnd.NextDouble();
            while (flt1 > MinPrice) flt1 = OpenPrice - 10 + 30 * rnd.NextDouble();

            double flt2 = OpenPrice - 10 + 30 * rnd.NextDouble();
            while (flt2 < MaxPrice) flt2 = OpenPrice - 10 + 30 * rnd.NextDouble();

            MinPrice = flt1;
            MaxPrice = flt2;
        }

        static int GetCurrentAssetSession(int ID_Asset)
        {
            DataSet ds = bitSimpleSQL.Query("SELECT TOP 1 * FROM [AssetSessions] WHERE ([ID_Asset] = " + ID_Asset.ToString() + ") ORDER BY [Date] DESC");
            if (!bitSimpleSQL.isValidDSRows(ds)) return 0;

            return Convert.ToInt32(ds.Tables[0].Rows[0]["ID"]);
        }

        static int GetPreviousAssetSession(int ID_Asset, int ID_CurrentAssetSession)
        {
            DataSet ds = bitSimpleSQL.Query("SELECT TOP 1 * FROM [AssetSessions] WHERE ([ID_Asset] = " + ID_Asset.ToString() + ") AND ([ID] < " + ID_CurrentAssetSession.ToString() + ") ORDER BY [Date] DESC");
            if (!bitSimpleSQL.isValidDSRows(ds)) return 0;

            return Convert.ToInt32(ds.Tables[0].Rows[0]["ID"]);
        }

        static void Main(string[] args)
        {
            while (!System.Console.KeyAvailable)
            {
                bitSimpleSQL.Connect("DB.xml");

                //  get ring parameters
                TimeSpan dt_OpeningTime = new TimeSpan(9, 0, 0);
                TimeSpan dt_PreClosingTime = new TimeSpan(9, 0, 0);
                DataSet ds_ring = bitSimpleSQL.Query("SELECT TOP 1 * FROM [Rings] WHERE [ID] = 1");
                if (bitSimpleSQL.isValidDSRows(ds_ring))
                {
                    dt_OpeningTime = (TimeSpan)ds_ring.Tables[0].Rows[0]["OpeningTime"];
                    dt_PreClosingTime = (TimeSpan)ds_ring.Tables[0].Rows[0]["PreClosingTime"];
                }

                //  get current asset session
                int ID_AssetSession = GetCurrentAssetSession(1);
                if (ID_AssetSession > 0)
                {
                    //  get last market data entry
                    DataSet ds_marketdata = bitSimpleSQL.Query("SELECT TOP 1 * FROM [MarketData] WHERE [ID_AssetSession] = " + ID_AssetSession.ToString() + " ORDER BY [Tick] DESC");
                    if (bitSimpleSQL.isValidDS(ds_marketdata))
                    {
                        double flt_open = 0;
                        double flt_min = 0;
                        double flt_max = 0;
                        double flt_close = 0;

                        TimeSpan currentTick;

                        if (!bitSimpleSQL.isValidDSRows(ds_marketdata))
                        {
                            //  retrieve the last recorded price in market data
                            int ID_PreviousSession = GetPreviousAssetSession(1, ID_AssetSession);
                            if (ID_PreviousSession > 0)
                            {
                                DataSet ds_prevclose = bitSimpleSQL.Query("SELECT TOP 1 * FROM [MarketData] WHERE ([ID_AssetSession] = " + ID_PreviousSession.ToString() + ") ORDER BY [Tick] DESC");
                                if (bitSimpleSQL.isValidDSRows(ds_prevclose)) flt_open = Convert.ToDouble(ds_prevclose.Tables[0].Rows[0]["Close"]);
                            }
                            else flt_open = 0;

                            //  make first entry in the session
                            currentTick = dt_OpeningTime; //  new TimeSpan(9, 0, 0);
                            //Calculate5minRND(currentTick, ref flt_open, ref flt_min, ref flt_max, ref flt_close);
                        }
                        else
                        {
                            //  continue to write the next entry
                            flt_open = Convert.ToDouble(ds_marketdata.Tables[0].Rows[0]["Close"]);
                            currentTick = ((TimeSpan)ds_marketdata.Tables[0].Rows[0]["Tick"]).Add(new TimeSpan(0, 5, 0));
                            //Calculate5minRND(currentTick, ref flt_open, ref flt_min, ref flt_max, ref flt_close);
                        }

                        if (currentTick >= dt_PreClosingTime) break;
                        if (currentTick <= DateTime.Now.TimeOfDay)
                            bitSimpleSQL.Execute("EXEC [CalculateMarketData5min] " + ID_AssetSession.ToString() );
                        /*bitSimpleSQL.Execute(@"INSERT INTO [MarketData] ([ID_AssetSession], [Tick], [Open], [Min], [Max], [Close]) VALUES 
                                             (" + ID_AssetSession.ToString() + ", '" + currentTick.ToString() + "', ROUND(" + flt_open.ToString() + ", 2), ROUND(" + flt_min.ToString() + ", 2), ROUND(" + flt_max.ToString() + ", 2), ROUND(" + flt_close.ToString() + ", 2))");*/
                    }
                }

                bitSimpleSQL.Disconnect();

                System.Threading.Thread.Sleep(10000);
            }
        }
    }
}
