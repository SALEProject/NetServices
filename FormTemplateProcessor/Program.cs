using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using NetServices;

namespace FormTemplateProcessor
{
    class Program
    {
        public class TForm
        {
            public string Type = "";
            public string DataType = "";
            public string Validation = "";
            public bool isMandatory = false;
            public string Dependency = "";
            public string Field = "";
            public string Values = "";
            public string Html = "";
            public string Style = "";
            public string Data = "";
            public TForm[] Items = null;
        }

        public static DateTime DBNow = DateTime.Now;

        static DateTime GetDBNow()
        {
            string sql = "SELECT GetDate() AS [Now]";
            DataSet ds_Now = bitSimpleSQL.Query(sql);
            if (ds_Now == null) return DateTime.Now;
            if (ds_Now.Tables.Count == 0) return DateTime.Now;
            if (ds_Now.Tables[0].Rows.Count == 0) return DateTime.Now;

            return Convert.ToDateTime(ds_Now.Tables[0].Rows[0]["Now"]);
        }

        static void ExtractAttributes(XmlNode Node, ref TForm frm)
        {
            for (int i = 0; i < Node.Attributes.Count; i++)
            {
                string name = Node.Attributes[i].Name.ToUpper();

                switch (name)
                {
                    case "FIELD":
                        frm.Field = Node.Attributes[i].InnerText;
                        break;
                    case "VALUES":
                        frm.Values = Node.Attributes[i].InnerText;
                        break;
                    case "STYLE":
                        frm.Style = Node.Attributes[i].InnerText;
                        break;
                }

            }
        }

        static TForm ProcessFormNode(XmlNode Node)
        {
            if (Node is XmlText) return null;

            string NodeName = Node.Name.ToUpper();
            TForm res = new TForm();
            res.Items = new TForm[0];

            switch (NodeName)
            {
                case "P":
                    res.DataType = "html";
                    res.Type = "paragraph";
                    if (Node.HasChildNodes && Node.ChildNodes[0] is XmlText)
                        res.Html = Node.InnerText;
                    ExtractAttributes(Node, ref res);
                    break;
                case "CENTER":
                case "STRONG":
                case "SPAN":
                    res.DataType = "html";
                    res.Type = Node.Name;
                    if (Node.HasChildNodes && Node.ChildNodes[0] is XmlText)
                        res.Html = Node.InnerText;
                    ExtractAttributes(Node, ref res);
                    break;
                case "BR":
                    res.DataType = "html";
                    res.Type = "tag";
                    res.Html = "<br/>";
                    break;
                case "TEXTBOX":
                    res.DataType = "string";
                    res.Type = "textbox";
                    ExtractAttributes(Node, ref res);
                    break;
                case "SELECT":
                    res.Type = "select";
                    ExtractAttributes(Node, ref res);
                    break;
                default:
                    return null;
            }

            foreach (XmlNode xml_node in Node.ChildNodes)
            {
                TForm frm_node = ProcessFormNode(xml_node);
                if (frm_node != null)
                {
                    TForm[] items = new TForm[res.Items.Length + 1];
                    for (int i = 0; i < res.Items.Length; i++) items[i] = res.Items[i];
                    items[res.Items.Length] = frm_node;
                    res.Items = items;
                }
            }

            return res;
        }

        static void SaveFormField(int ID_Form, int ID_Parent, int Order, TForm Field)
        {
            int b_Mandatory = 0;
            if (Field.isMandatory) b_Mandatory = 1;
            bitSimpleSQL.Execute("INSERT INTO [FormFields] ([ID_Form], [ID_Parent], [Order], [Type], [DataType], [Validation], [isMandatory], [Dependency], [Field], [Values], [Html], [Style]) " +
                                 "VALUES (" + ID_Form.ToString() + ", " + ID_Parent.ToString() + ", " + Order.ToString() + ", '" + Field.Type + "', '" + Field.DataType + "', '', " + b_Mandatory.ToString() + ", '', '" + Field.Field + "', '" + Field.Values + "', '" + Field.Html + "', '" + Field.Style + "')");
            DataSet ds_id = bitSimpleSQL.Query("SELECT @@Identity AS [ID]");
            if (bitSimpleSQL.isValidDSRows(ds_id))
            {
                int ID = Convert.ToInt32(ds_id.Tables[0].Rows[0]["ID"]);
                for (int i = 0; i < Field.Items.Length; i++)
                {
                    SaveFormField(ID_Form, ID, i, Field.Items[i]);
                }
            }
        }

        static string ProcessTemplate(int ID_Form, string str_Template)
        {
            string str_xml = "";
            try
            {
                byte[] data = Convert.FromBase64String(str_Template);
                str_xml = Encoding.UTF8.GetString(data);
            }
            catch (Exception exc)
            {
                return "Conversion Failed";
            }

            XmlDocument xml_doc = new XmlDocument();
            try
            {
                xml_doc.LoadXml(str_xml);
            }
            catch (Exception exc)
            {
                return "XML Structure Invalid";
            }

            TForm frm = new TForm();
            frm.Items = new TForm[0];
            foreach (XmlNode xml_node in xml_doc.DocumentElement.ChildNodes)
            {
                TForm frm_node = ProcessFormNode(xml_node);
                if (frm_node != null)
                {
                    TForm[] items = new TForm[frm.Items.Length + 1];
                    for (int i = 0; i < frm.Items.Length; i++) items[i] = frm.Items[i];
                    items[frm.Items.Length] = frm_node;
                    frm.Items = items;
                }
            }

            bitSimpleSQL.Execute("DELETE FROM [FormFields] WHERE [ID_Form] = " + ID_Form.ToString());
            for (int i = 0; i < frm.Items.Length; i++) SaveFormField(ID_Form, 0, i, frm.Items[i]);

            return "";
        }

        static void Main(string[] args)
        {
            bitSimpleSQL.Connect("DB.xml");

            Random rnd = new Random(DateTime.Now.Millisecond);
            string sql = "";
            int counter = 0;

            while (!Console.KeyAvailable)
            {
                if (counter % 10000 == 0)
                {
                    DBNow = GetDBNow();

                    try
                    {
                        DataSet ds = bitSimpleSQL.Query("SELECT * FROM [Forms] WHERE ([Status] = 'pending') AND ([Template] IS NOT NULL)");
                        if (bitSimpleSQL.isValidDS(ds))
                        {
                            foreach (DataRow row in ds.Tables[0].Rows)
                            {
                                int ID = Convert.ToInt32(row["ID"]);
                                bitSimpleSQL.Execute("UPDATE [Forms] SET [Status] = 'processing' WHERE [ID] = " + ID.ToString());

                                string Template = row["Template"].ToString();
                                string error = ProcessTemplate(ID, Template);

                                if (error == "")
                                    bitSimpleSQL.Execute("UPDATE [Forms] SET [Status] = 'processed' WHERE [ID] = " + ID.ToString());
                                else
                                    bitSimpleSQL.Execute("UPDATE [Forms] SET [Status] = 'error' WHERE [ID] = " + ID.ToString());
                            }
                        }
                    }
                    catch (Exception exc)
                    {
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
