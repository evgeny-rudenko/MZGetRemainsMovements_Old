using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.IO;
using System.Data.SqlClient;
using System.Reflection;


namespace MZGetRemainsMovements
{
    class Program
    {
        /// <summary>
        /// Записываем в файл строку
        /// </summary>
        /// <param name="LineToAdd">Строка или строки для записи</param>
        /// <param name="sFile">Наименование файла</param>
        /// <returns></returns>
        private static int AddLine(String LineToAdd, String sFile)
        {
            

            Encoding enc = Encoding.GetEncoding(1251);
            //using (StreamWriter sw = File.AppendText(Path.Combine( Environment.CurrentDirectory, sFile)))
            using (StreamWriter sw = new StreamWriter(Path.Combine(Directory.GetCurrentDirectory().ToString(), sFile), true, Encoding.GetEncoding(1251)))
            {

                
                sw.Write(LineToAdd);

            }


            return 0;
        }



        static void Main(string[] args)
        { 
            ///помощь для пользователя - что и как запускать
            #region
            if (args.Length ==0)
            {
                Console.WriteLine("Не заданы параметры ");
                Console.WriteLine("Примеры использования");
                Console.WriteLine("mzgetremainsmovements.exe store                                      - получить список складов");
                Console.WriteLine("mzgetremainsmovements.exe 01.01.2010 25.08.2017 1                    - выгрузить данные");
                Console.WriteLine("mzgetremainsmovements.exe начальная_дата конечная_дата код_склада    - выгрузить данные");
                Console.WriteLine("mzgetremainsmovements.exe store                                      - получить список складов");
                return;
            }

            if (args.Length == 1 && args[0] == "store")
            {

                using (SqlConnection connection = new SqlConnection(Properties.Settings.Default.ConnectionString))
                {
                    connection.Open();
                    using (var cmd = connection.CreateCommand())
                    {
                        cmd.CommandText = "select ID_STORE , NAME from STORE";// File.ReadAllText(tableName);
                        using (var reader = cmd.ExecuteReader())
                        {
                            
                            while (reader.Read())
                            {
                                string line = "";
                                string v="";
                                for (int i = 0; i < reader.FieldCount; i++)
                                {

                                    
                                    v = reader[i].ToString();
                                    line = line +v + "; ";
                                }
                                Console.WriteLine(line);
                            }
                        }
                    }
                    return;
                }
            }
            #endregion

           


            var assembly = Assembly.GetExecutingAssembly();
            string datefirst, datelast, store;

            datefirst = args[0];
            datelast = args[1];
            store = args[2];

            /// заполняем массив с кодами складов
            List<String> stores = new List<String>();
            if (Properties.Settings.Default.AllStore == true)
            {

                using (SqlConnection connection = new SqlConnection(Properties.Settings.Default.ConnectionString))
                {
                    connection.Open();
                    using (var cmd = connection.CreateCommand())
                    {
                        cmd.CommandText = "select ID_STORE  from STORE";// File.ReadAllText(tableName);
                        using (var reader = cmd.ExecuteReader())
                        {
                            int i = 0;
                            while (reader.Read())
                            {

                                stores.Add(reader[i].ToString());
                                //stores =  reader[i].ToString();

                            }
                        }
                    }

                }
            }
            else
            {
                stores.Add(store);
            }

            foreach (String store_ in stores)
            {
                String[] ResNames = assembly.GetManifestResourceNames();
                var resourceName = ResNames[0];

                using (Stream stream = assembly.GetManifestResourceStream(resourceName))
                using (TextReader reader = new StreamReader(stream))
                {

                    String result = reader.ReadToEnd();
                    result = result.Replace("!datefirst!", datefirst);
                    result = result.Replace("!datelast!", datelast);
                    result = result.Replace("!store!", store_);


                    using (SqlConnection connection = new SqlConnection(Properties.Settings.Default.ConnectionString))
                    {
                        connection.Open();
                        ExportTable(connection, result, store_ + "$" + datefirst + "-" + datelast + ".mov");

                    }
                }

                resourceName = ResNames[1];

                using (Stream stream = assembly.GetManifestResourceStream(resourceName))
                using (TextReader reader = new StreamReader(stream))
                {

                    String result = reader.ReadToEnd();
                    result = result.Replace("!datefirst!", datelast);
                    result = result.Replace("!datelast!", datelast);
                    result = result.Replace("!store!", store_);
                    using (SqlConnection connection = new SqlConnection(Properties.Settings.Default.ConnectionString))
                    {
                        connection.Open();
                        ExportTable(connection, result, store_ + "$" +datefirst +"-" +datelast +  ".ost");

                    }
                }
                // Console.ReadKey();

            }
        }


        private static void ExportTable(SqlConnection connection, string tableName, string fName)
        {
            //fName = DateTime.Now.ToString("yyyy-MM-dd ") + "~" + Properties.Settings.Default.SubID + fName;
            // fName = DateTime.Now.ToString() + "~" + Properties.Settings.Default.SubID + fName;
           // fName = DateTime.Now.ToString() + "~" + fName;

            String pPath = Directory.GetCurrentDirectory();
            if (Properties.Settings.Default.DestinationFolder.Length > 1)
            {
                pPath = Properties.Settings.Default.DestinationFolder;
            }
            fName = fName.Replace(" ", "-");
            fName = fName.Replace(":", "-");


            Console.WriteLine("Writing " + fName);
            using (var output = new StreamWriter(Path.Combine(pPath, fName), false, Encoding.GetEncoding("Windows-1251"))) // добавить дату fname
            {
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandTimeout = 0;
                    cmd.CommandText = tableName;// File.ReadAllText(tableName);
                    try
                    {
                        using (var reader = cmd.ExecuteReader())
                        {
                            // WriteHeader(reader, output);
                            while (reader.Read())
                            {
                                WriteData(reader, output);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(DateTime.Now.ToString());
                        Console.Write(ex.ToString());
                        AddLine(ex.ToString(),  "log.txt");
                        AddLine(cmd.CommandText,"logsql.txt");
                    }
                }
            }
        }


        private static void WriteHeader(SqlDataReader reader, TextWriter output)
        {
            for (int i = 0; i < reader.FieldCount; i++)
            {
                if (i > 0)
                    output.Write(';');
                output.Write(reader.GetName(i));
            }
            output.Write(';');
            output.WriteLine();
        }

        private static void WriteData(SqlDataReader reader, TextWriter output)
        {
            for (int i = 0; i < reader.FieldCount; i++)
            {
                if (i > 0)
                    output.Write(';');
                String v = reader[i].ToString();
                if (reader[i].GetType().FullName == "System.Decimal")
                    v = v.Replace(",", ".");

                if (v.Contains(';') || v.Contains('\n') || v.Contains('\r') || v.Contains('"'))
                {
                    //output.Write('"');
                    output.Write(v.Replace("\"", "\"\""));
                    //output.Write('"');
                }
                else
                {

                    output.Write(v);
                }
            }
            output.Write(";");
            output.WriteLine();
        }
    }


}

