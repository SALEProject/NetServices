using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NetServices;

namespace DocumentGenerator
{
    class Program
    {
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

        static void GenerateExportTemplate(int ID_Procedure, int ID_Form, int ID_ExportTemplate)
        {
            DataSet ds_form = bitSimpleSQL.Query("SELECT [Name], [ExportTemplate] FROM [Forms] WHERE [ID] = " + ID_Form.ToString());
            if (!bitSimpleSQL.isValidDSRows(ds_form)) return;

            string str_name = ds_form.Tables[0].Rows[0]["Name"].ToString();
            string str_template = ds_form.Tables[0].Rows[0]["ExportTemplate"].ToString();
            if (str_template == "") return;
            byte[] data_template = Convert.FromBase64String(str_template);
            str_template = Encoding.UTF8.GetString(data_template);

            //  get parameters and change them in the template
            DataSet ds_params = bitSimpleSQL.Query("EXEC [GetProcedureVariables] " + ID_Procedure.ToString());
            if (bitSimpleSQL.isValidDSRows(ds_params))
                foreach (DataRow row in ds_params.Tables[0].Rows)
                {
                    string str_key = row["Key"].ToString();
                    string str_value = row["Value"].ToString();
                    str_value = str_value.Replace("\n", "<br />");
                    str_template = str_template.Replace(str_key, str_value);
                }

            string str_content = "";
            string[] content = new string[1];
            content[0] = str_template;
            string filename = DateTime.Now.ToString().GetHashCode().ToString("x");
            System.IO.File.WriteAllLines(filename + ".html", content);
            //System.Diagnostics.Process prc = System.Diagnostics.Process.Start("D:\\LibreOfficePortable\\App\\libreoffice\\program\\soffice.exe", "--headless --convert-to docx:\"MS Word 2003 XML\" " + filename + ".html");
            //:\"MS Word 2003 XML\"
            //docx:\"MS Word 2003 XML\"
            //prc.WaitForExit(5000);
            if (System.IO.File.Exists(filename + ".html"))
            {
                byte[] data = System.IO.File.ReadAllBytes(filename + ".html");
                str_content = Convert.ToBase64String(data);
            }

            System.IO.File.Delete(filename + ".html");
            System.IO.File.Delete(filename + ".docx");

            if (ID_ExportTemplate == 0)
                bitSimpleSQL.Execute("INSERT INTO [ProcedureExportTemplates] ([ID_Procedure], [ID_Form], [FileName], [Content]) VALUES (" + ID_Procedure.ToString() + ", " + ID_Form.ToString() + ", '" + str_name + "' + '.doc', '" + str_content + "')");
            else
                bitSimpleSQL.Execute("UPDATE [ProcedureExportTemplates] SET [Date_Generated] = GetDate(), [FileName] = '" + str_name + "' + '.doc', [Content] = '" + str_content + "' WHERE [ID] = " + ID_ExportTemplate.ToString()); 
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
                        string str_sql = 
                        @"
                        SELECT P.""ID"" AS ""ID_Procedure"", F.""ID"" AS ""ID_Form"", IsNull(PET.""ID"", 0) AS ""ID_ExportTemplate"" 
                        FROM ""Procedures"" P
                        LEFT JOIN ""Forms"" F ON (F.""isActive"" = 1) AND (F.""isDeleted"" = 0)
                        LEFT JOIN ""ProcedureExportTemplates"" PET ON (P.""ID"" = PET.""ID_Procedure"") AND (F.""ID"" = PET.""ID_Form"")
                        WHERE (P.""ID"" > 40) AND (P.""Status"" = 'draft') AND (P.""Date"" > IsNull(PET.""Date_Generated"", CAST('19000101' AS DATETIME)))
                        ";
                        DataSet ds_templates = bitSimpleSQL.Query(str_sql);
                        if (bitSimpleSQL.isValidDS(ds_templates))
                            foreach (DataRow row in ds_templates.Tables[0].Rows)
                            {
                                int ID_Procedure = Convert.ToInt32(row["ID_Procedure"]);
                                int ID_Form = Convert.ToInt32(row["ID_Form"]);
                                int ID_ExportTemplate = Convert.ToInt32(row["ID_ExportTemplate"]);

                                GenerateExportTemplate(ID_Procedure, ID_Form, ID_ExportTemplate);
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
