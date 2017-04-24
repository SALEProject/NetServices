using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Collections;
using System.Text.RegularExpressions;
using NetServices;
using System.Data;

namespace Warranties
{
    class Program
    {
        private static TTransaction transaction = new TTransaction("", "", 0, "", "", "");
        private static TSTA sta = new TSTA(new DateTime(), "", "", 0);

        public struct TSTA
        {
            public DateTime creationTime;
            public string filePath;
            public string fileName;
            public int ID_BCRSTA;

            public TSTA(DateTime creationTime, string filePath, string fileName, int ID_BCRSTA)
            {
                this.creationTime= creationTime;
                this.filePath = filePath;
                this.fileName = fileName;
                this.ID_BCRSTA = ID_BCRSTA;
            }
        }

        public struct TTransaction
        {
            public string date;
            public string direction;
            public double amount;
            public string type;
            public string id;
            public string description;

            public TTransaction(string date, string direction, double amount, string type, string id, string description)
            {
                this.date = date;
                this.direction = direction;
                this.amount = amount;
                this.type = type;
                this.id = id;
                this.description = description;
            }
        }

        public struct TPartner
        {
            public string account;
            public string company;
            public string fiscalCode;
            public string bank;

            public TPartner(string account, string company, string fiscalCode, string bank)
            {
                this.account = account;
                this.company = company;
                this.fiscalCode = fiscalCode;
                this.bank = bank;
            }
        }

        public struct TWarranty
        { 
            public int ID_PaymentType;
            public string WarrantyNumber;
            public int ID_Client;
            public int ID_Asset;
            public int ID_CreatedByUser;
            public int ID_Currency;
            public double ValueInCurrency;
            public double ExchangeRate;
            public DateTime Date;

            public TWarranty(int ID_PaymentType, string WarrantyNumber, int ID_Client, int ID_Asset, int ID_CreatedByUser, int ID_Currency, double ValueInCurrency, double ExchangeRate, DateTime Date)
            {
                this.ID_PaymentType = ID_PaymentType;
                this.WarrantyNumber = WarrantyNumber;
                this.ID_Client = ID_Client;
                this.ID_Asset = ID_Asset;
                this.ID_CreatedByUser = ID_CreatedByUser;
                this.ID_Currency = ID_Currency;
                this.ValueInCurrency = ValueInCurrency;
                this.ExchangeRate = ExchangeRate;
                this.Date = Date;
            }
        }

        static void Main(string[] args)
        {
            bitSimpleSQL.Connect("DB.xml");
            string path = "D:\\Projects\\BRM\\SALE\\Implementation\\BCR\\MCCWIN";
           // string path = "D:\\BCR\\MCCWIN";

            while (!Console.KeyAvailable)
            {

                if (File.Exists(path))
                {
                    ProcessFile(path);
                }
                else if (Directory.Exists(path))
                {
                    ProcessDirectory(path);
                }
                else
                {
                    Console.WriteLine("{0} is not a valid file or directory.", path);
                }

                Console.ReadLine();

                System.Threading.Thread.Sleep(60000);
            }

            bitSimpleSQL.Disconnect();
        }

        // Process all files in the directory passed in, recurse on any directories  
        // that are found, and process the files they contain. 
        public static void ProcessDirectory(string targetDirectory)
        {
            int i = 0;
            // Process the list of files found in the directory. 
            //string[] fileEntries = Directory.GetFiles(targetDirectory);
            //var files = Directory.GetFiles(targetDirectory, "*.*", SearchOption.AllDirectories).Where(s => s.EndsWith(".STA") || s.EndsWith(".VML"));
            string[] fileEntries = Directory.GetFiles(targetDirectory, "*.*", SearchOption.AllDirectories).Where(s => s.EndsWith(".STA") || s.EndsWith(".VML")).ToArray();
            foreach (string fileName in fileEntries)
            {
                //just verify that the current file wasn't processed before
                string sql = "SELECT * FROM [BCR_STA] WHERE ([FilePath] = '" + fileName + "') AND ([isProcessed] = 1)";
                DataSet ds_STA = bitSimpleSQL.Query(sql);
                if ((ds_STA != null) && (ds_STA.Tables.Count > 0) && (ds_STA.Tables[0].Rows.Count > 0))
                    continue;

                FileInfo fi = null;
                try
                {
                    fi = new FileInfo(fileName);
                    sta.creationTime = fi.CreationTime;
                    sta.fileName = fi.Name;
                    sta.filePath = fi.FullName;
                    sql = "INSERT INTO [BCR_STA] ([FilePath], [FileName], [FileCreationDate], [isProcessed]) VALUES ('" + fi.FullName + "', '" + fi.Name + "', '" + fi.CreationTime + "', 1)";
                    bitSimpleSQL.Execute(sql);

                    sql = "SELECT TOP 1 * FROM [BCR_STA] ORDER BY [ID] DESC";
                    ds_STA = bitSimpleSQL.Query(sql);
                    if (ds_STA == null) throw new FileNotFoundException(fileName);
                    if (ds_STA.Tables.Count == 0) throw new FileNotFoundException(fileName);
                    int ID_BCRSTA = Convert.ToInt32(ds_STA.Tables[0].Rows[0]["ID"]);

                    sta = new TSTA(fi.CreationTime, fi.FullName, fi.Name, ID_BCRSTA);
                }
                catch (FileNotFoundException ex)
                {
                    Console.WriteLine("Error reading file {0} : {1}", fi.Name, fi.Directory);
                    sql = "INSERT INTO [BCR_STA_Exceptions] ([ID_BCRSTA], [FilePath], [Exception]) VALUES (" + sta.ID_BCRSTA.ToString() + ", '" + sta.fileName + "', 'Error reading file or getting the file id.')";
                    bitSimpleSQL.Execute(sql);
                }

                ProcessFile(fileName);
                i++;
            }
        }

        // Insert logic for processing found files here. 
        public static void ProcessFile(string path)
        {
            //Console.WriteLine("Processed file '{0}'.", path);
            int counter = 0;
            try
            {     
                string line;

                // Read the file and display it line by line.
                System.IO.StreamReader file = new System.IO.StreamReader(path);

                while ((line = file.ReadLine()) != null)
                {
                    ProcessLine(line);
                    counter++;
                }
                
                file.Close();

                // Suspend the screen.
                //Console.ReadLine();
            }
            catch (Exception ex)
            { 
                string exception = "Error reading line: " + counter.ToString();
                Console.WriteLine(exception);
                string sql = "INSERT INTO [BCR_STA_Exceptions] ([ID_BCRSTA], [FilePath], [Exception]) VALUES (" + sta.ID_BCRSTA.ToString() + ", '" + sta.fileName + "', '" + exception + "')";
                bitSimpleSQL.Execute(sql);
            }
        }
        
        public static void ProcessLine(string line)
        {
            string str = "";
            string code = "";
            string[] lineParts; 

            //1. detect the code length 2 or 3 chars (ex: 61 / 61F)
            //1.1 we know that each code is surrounded by 2 : (ex: :61:)
            int count = line.Split(':').Length - 1;
            if (count == 2)
            {
                // the this is a line that starts with a code
                //1.2 detect the code
                lineParts = line.Split(':');
                code = lineParts[1];
                str = lineParts[2];
            }
            else
                code = "-1";
            
            //2. interpret the code
            switch (code)
            { 
                case "20":
                    //:20: referinta (din data (AALLZZ) + cod unitate bancara pe 2 caractere)
                    break;
                case "25":
                    //:25: OIS_BCR/cont (5 caractere / maxim 24 caractere)
                    break;
                case "28":
                    //:28: pointer / pagina (5 caractere / 2 caractere)
                    break;
                case "60F":
                    //:60F: C (sau D) AALLZZ WWW (adica ROL) suma (15 caractere cu 2 zecimale)
                    break;
                case "61":
                    ProcessTransaction();
                    //:61: AALLZZ C (sau D) suma NTRF referinta cont operatiune
                    //line with code :61: looks similar to this one - :61:1504200420CN229684.53NTRF//2015042005021466
                    //from this line we can extract the following informations:
                    //1504200420 - YYMMDD + 4 digits (not documented, maybe MMDD or hh:mm(? - hard to believe))
                    //C - (not documented) but after the files analysis we know that transactions with generate income. Transactions with D generate outcome
                    //N - (not documented) but will be used to split the strings. there are file where this N is "missing"(?)
                    //229684.53 - between C/D/N and NTRF or NCOM is the amount of money transacted
                    //2015042008065638 - "referinta cont operatiune". this will be our semi-unique transaction identifier (semiunique because of commisions transactions). 
                    
                    //we already have the string without the code in lineParts[2] or str
                    //some string cleanup remove "NONREF" and "NOTPROVIDED", "NOT PROVIDED" etc
                    str = str.Replace("NONREF", "");
                    str = str.Replace("NOTPROVIDED", "");
                    str = str.Replace("NOT PROVIDED", "");
                    str = str.Replace("NONE", "");

                    try
                    {
                        string date = "";
                        string transactionType = "";    //can be TRF (transaction) or COM (commision)
                        string transactionDirection = ""; //can be C or D
                        double amount = 0;
                        string transactionID = "";

                        date = str.Substring(0, 10); // "date" it's something like 
                        date = "20" + date.Substring(0, 6);
                        str = str.Remove(0, 10);

                        if (str.Contains("NTRF"))
                            transactionType = "TRF"; //transaction
                        if (str.Contains("NCOM"))
                            transactionType = "COM"; //commision

                        string[] parts = str.Split('N');

                        //there are cases when code 61 contains CN/DN or just C/D
                        //when you split the string by 'N', you will either have something like parts[0] = C; parts[1] = 235689.89 or parts[0] = C235689.89
                        if ((parts[0] == "C") || (parts[0] == "D"))
                        {
                            transactionDirection = parts[0];
                            amount = Convert.ToDouble(parts[1]);
                            transactionID = parts[2].Split('/')[2];
                        }
                        else
                        {
                            transactionDirection = parts[0].Substring(0, 1);
                            parts[0] = parts[0].Remove(0, 1);
                            amount = Convert.ToDouble(parts[0]);
                            transactionID = parts[1].Split('/')[2];
                        }

                        transaction = new TTransaction(date, transactionDirection, amount, transactionType, transactionID, "");
                    }
                    catch (Exception ex)
                    {

                    }

                    break;
                case "86":
                    //:86: detalii de plata
                    //actually this case is pretty easy. 86 contains the description but this descriptions comes on multiple rows. The second/third/etc row  doesn't have any code at the beggining so we will asume that any row without code is in fact part of the description of the current transaction
                    transaction.description = line;
                    break;
                case "62F":
                    //:62F: C (sau D) AALLZZ WWW (adica ROL) suma (15 caractere cu 2 zecimale)
                    break;
                case "-1":
                    transaction.description += line;
                    break;
            }
        }

        public static void ProcessTransaction()
        {
            if (transaction.type == "TRF")
            { 
                //we can identify both parties
                //the transaction description contains some keywords like "Platitor", "CODFISC", "Beneficiar, "Detalii"
                try
                {
                    if (transaction.description.Contains("Inchidere depozit"))
                        return;

                    string[] parts = transaction.description.Split(new string[] { "-Platitor" }, StringSplitOptions.None);
                    string supplierDetails = parts[1].Split(new string[] { "-Beneficiar" }, StringSplitOptions.None)[0];
                    string beneficiaryDetails = parts[1].Split(new string[] { "-Beneficiar" }, StringSplitOptions.None)[1].Split(new string[] { "-Detalii" }, StringSplitOptions.None)[0];
                    string details = parts[1].Split(new string[] { "-Beneficiar" }, StringSplitOptions.None)[1].Split(new string[] { "-Detalii" }, StringSplitOptions.None)[1];

                    parts = supplierDetails.Split(new string[] { "CODFISC" }, StringSplitOptions.None);
                    TPartner supplier = ProcessPartner(parts);

                    parts = beneficiaryDetails.Split(new string[] { "CODFISC" }, StringSplitOptions.None);
                    TPartner beneficiary = ProcessPartner(parts);

                    string sql = "";
                    sql = "SELECT * FROM [BO_OperationsBuffer] WHERE [BankTransactionID] = '" + transaction.id + "'";
                    bitSimpleSQL.Execute(sql);
                    DataSet ds_Buffer = bitSimpleSQL.Query(sql);
                    if ((ds_Buffer == null) || (ds_Buffer.Tables.Count == 0) || (ds_Buffer.Tables[0].Rows.Count == 0))
                    {
                        //the transaction wasn't processed before
                        sql =   "INSERT INTO [BO_OperationsBuffer] " +
                                "([ID_BCRSTA], [BankTransactionID], [TransactionDate], [TransactionAmount], [TransactionType], [TransactionDetails], [SupplierName], [SupplierBank], [SupplierBankAccount], [SupplierFiscalCode], [BeneficiaryName], [BeneficiaryBank], [BeneficiaryBankAccount], [BeneficiaryFiscalCode]) " +
                                "VALUES " +
                                "("+ sta.ID_BCRSTA.ToString() +", '" + transaction.id + "', '" + transaction.date + "', " + transaction.amount + ", '" + transaction.type + "', '" + details + "', '" + supplier.company + "', '" + supplier.bank + "', '" + supplier.account + "', '" + supplier.fiscalCode + "', '" + beneficiary.company + "', '" + beneficiary.bank + "', '" + beneficiary.account + "', '" + beneficiary.fiscalCode + "')";
                        bitSimpleSQL.Execute(sql);
                        Console.WriteLine("Transaction processed. BankID: " + transaction.id + " From: " + supplier.account + " To: " + beneficiary.account);
                    }

                    //Each agency will have a client code printed in account administration, ex: COD_CLIENT:125
                    if (details.Contains("COD_CLIENT"))
                    { 
                        parts = details.Split(new string[] { " " }, StringSplitOptions.None);
                        for (int i = 0; i < parts.Length; i++)
                        {
                            if (parts[i].Contains("COD_CLIENT"))
                            {
                                string clientCode = parts[i];
                                int clientID = Convert.ToInt32(clientCode.Split(':')[1]);

                                //ID_PaymentType = 9; numerar - WarrantyTypes
                                //ID_CreatedbyUser = 1 admin
                                //ID_Currency = 1 lei

                                TWarranty warranty = new TWarranty(9, transaction.id, clientID, 0, 1, 1, transaction.amount, 1, DateTime.ParseExact(transaction.date, "yyyymmdd", System.Globalization.CultureInfo.InvariantCulture));
                                addWarranty(warranty);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    string exception = "Unknown description/text: " + transaction.description;
                    Console.WriteLine(exception);
                    string sql = "INSERT INTO [BCR_STA_Exceptions] ([ID_BCRSTA], [FilePath], [Exception]) VALUES (" + sta.ID_BCRSTA.ToString() + ", '" + sta.fileName + "', '" + exception + "')";
                    bitSimpleSQL.Execute(sql);
                }
            }

            transaction = new TTransaction("", "", 0, "", "", "");
        }

        public static TPartner ProcessPartner(string[] parts)
        {
            string account = "";
            string company = "";
            string fiscalCode = "";
            string bank = "";

            parts[0] = parts[0].TrimEnd(' ');
            string companyAndAccount = parts[0];
            account = companyAndAccount.Split(' ')[companyAndAccount.Split(' ').Length - 1].TrimStart(' ').TrimEnd(' ');
            company = companyAndAccount.Replace(account, "").TrimStart(' ').TrimEnd(' ');

            if (parts.Length > 1)
            {
                parts[1] = parts[1].TrimStart(' ');
                string fiscalCodeAndBank = parts[1];
                fiscalCode = fiscalCodeAndBank.Split(' ')[0].TrimStart(' ').TrimEnd(' ');
                bank = fiscalCodeAndBank.Replace(fiscalCode, "").TrimStart(' ').TrimEnd(' ');
            }

            return new TPartner(account, company, fiscalCode, bank);
        }

        public static void addWarranty(TWarranty warranty)
        {
            DateTime ValabilityStartDate = DateTime.Now;
            DateTime ValabilityEndDate = DateTime.Now.AddDays(20);
            DateTime ExecutionDate = DateTime.Now.AddDays(30);

            try
            {
                string sql = "  INSERT INTO [BO_Operations] ([ID_OperationType], [Number], [Date], [DateClosed], [isClosed], [isCanceled], [Reference]) " +
                             "  VALUES ((SELECT [ID] FROM [BO_OperationTypes] WHERE [Code] = 'CRED_WAR'), (SELECT ISNULL(MAX([Number]), 0) + 1 FROM [BO_Operations]), GETDATE(), GETDATE(), 1, 0, '" + warranty.WarrantyNumber + "')";
                bitSimpleSQL.Execute(sql);

                sql = "SELECT TOP 1 * FROM [BO_Operations] ORDER BY [ID] DESC";
                DataSet ds_Operations = bitSimpleSQL.Query(sql);
                int ID_Operation = Convert.ToInt32(ds_Operations.Tables[0].Rows[0]["ID"]);               
               

                sql = "INSERT INTO [BO_OperationDetails_Payments] ([ID_PaymentType], [PaymentDescription], [Sum], [Percent], [isPayed], [PaymentDate], [ID_Currency], [ValabilityStartDate], [ValabilityEndDate], [ExecutionDate], [ExchangeRate])" +
                      "VALUES (" + warranty.ID_PaymentType.ToString() + ", '', " + warranty.ValueInCurrency.ToString() + ", 1, 1, '" + warranty.Date + "' , " + warranty.ID_Currency + ", '" + warranty.Date + "', '" + ValabilityEndDate + "', '" + ExecutionDate + "', " + warranty.ExchangeRate + ")";
                bitSimpleSQL.Execute(sql);

                sql = "SELECT TOP 1 * FROM [BO_OperationDetails_Payments] ORDER BY [ID] DESC";
                DataSet ds_Payments = bitSimpleSQL.Query(sql);
                int ID_OperationPayment = Convert.ToInt32(ds_Payments.Tables[0].Rows[0]["ID"]); 

                sql =        "  INSERT INTO [BO_OperationsXDetails] ([ID_Operation], [ID_Parent], [ID_InterNode], [Date], [ObjectType], [Description], [ID_OperationDetail], [ID_ClientSRC], [ID_ClientDEST], [ID_AgencySRC], [ID_AgencyDEST], [ID_Asset], [ID_CreatedByUser], [isCanceled]) " +
                             "  VALUES (" + ID_Operation.ToString() + ", 0, 0, GETDATE(), 2, 'Payment', " + ID_OperationPayment.ToString() + ", " + warranty.ID_Client.ToString() + ", 0, 0, 0, 0, " + warranty.ID_CreatedByUser + ", 0) ";
                bitSimpleSQL.Execute(sql);
            }
            catch (Exception exc)
            {

            }

        }
    }
}
