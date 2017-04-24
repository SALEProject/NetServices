using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Data;
using System.Data.SqlClient;

namespace NetServices
{
    class bitSimpleSQL
    {

        public static SqlConnection srv_connection = null;
        private static string FErrorMessage = "";
        private static bool FConnected;

        public static bool Connected
        {
            get
            {
                return FConnected;
            }
        }

        public static string ErrorMessage
        {
            get
            {
                return FErrorMessage;
            }
        }

        public static bool Connect(string server, string database, string user, string password)
        {
            string str = "server=" + server + ";database=" + database + ";uid=" + user + ";pwd=" + password + "";

            try
            {
                srv_connection = new SqlConnection(str);
                srv_connection.Open();
                FConnected = true;
                return true;
            }
            catch (Exception exc)
            {
                FErrorMessage = exc.Message;
                return false;
            }
        }

        public static bool Connect(string XMLFile)
        {
            string path = XMLFile;

            XmlDocument xml = new XmlDocument();
            try
            {
                string str_server = "";
                string str_database = "";
                string str_user = "";
                string str_password = "";

                try
                {
                    xml.Load(path);

                    for (int i = 0; i < xml.DocumentElement.ChildNodes.Count; i++)
                    {
                        XmlNode node = xml.DocumentElement.ChildNodes[i];
                        string node_name = node.Name.ToUpper();

                        switch (node_name)
                        {
                            case "SERVER":
                                str_server = node.InnerText;
                                break;
                            case "DATABASE":
                                str_database = node.InnerText;
                                break;
                            case "UID":
                                str_user = node.InnerText;
                                break;
                            case "PWD":
                                str_password = node.InnerText;
                                break;
                        }
                    }

                    return Connect(str_server, str_database, str_user, str_password);
                }
                catch (Exception exc)
                {
                    FErrorMessage = exc.Message;
                    return false;
                }
            }
            finally
            {
                xml = null;
            }
        }

        public static void Disconnect()
        {
            if (srv_connection != null)
            {
                srv_connection.Close();
                srv_connection.Dispose();
                FConnected = false;
            }
        }

        public static DataSet Query(string str_sql)
        {
            SqlCommand cmd = null;
            SqlDataAdapter adapter = null;

            try
            {
                cmd = new SqlCommand(str_sql, srv_connection);
                cmd.CommandType = CommandType.Text;

                adapter = new SqlDataAdapter(cmd);

                try
                {
                    DataSet ds = new DataSet();
                    adapter.Fill(ds);
                    return ds;
                }
                catch (Exception exc)
                {
                    FErrorMessage = exc.Message;
                    return null;
                }
            }
            finally
            {
                if (adapter != null) adapter.Dispose();
                if (cmd != null) cmd.Dispose();
            }
        }

        public static int Execute(string str_sql)
        {
            SqlCommand cmd = null;
            try
            {
                cmd = new SqlCommand(str_sql, srv_connection);

                SqlParameter param = new SqlParameter("param", SqlDbType.Binary);
                try
                {
                    return cmd.ExecuteNonQuery();
                }
                catch (Exception exc)
                {
                    FErrorMessage = exc.Message;
                    return -1;
                }
            }
            finally
            {
                if (cmd != null) cmd.Dispose();
            }
        }

        public static int Execute(string str_sql, byte[] blob_param)
        {
            SqlCommand cmd = null;
            try
            {
                cmd = new SqlCommand(str_sql, srv_connection);

                SqlParameter param = cmd.Parameters.Add("blob_param", SqlDbType.Binary);
                param.Value = blob_param;
                param.Direction = ParameterDirection.Input;

                try
                {
                    return cmd.ExecuteNonQuery();
                }
                catch (Exception exc)
                {
                    FErrorMessage = exc.Message;
                    return -1;
                }
            }
            finally
            {
                if (cmd != null) cmd.Dispose();
            }
        }

        public static bool isValidDS(DataSet ds)
        {
            if (ds == null) return false;
            if (ds.Tables.Count == 0) return false;

            return true;
        }

        public static bool isValidDSRows(DataSet ds)
        {
            if (ds == null) return false;
            if (ds.Tables.Count == 0) return false;
            if (ds.Tables[0].Rows.Count == 0) return false;

            return true;
        }
    }

}
