using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace FortisToAwdWindowsService
{
    public static class LogClass
    {
        public static void WriteLog(string message)
        {
            StreamWriter sw = null;
            try
            {
                sw = new StreamWriter(AppDomain.CurrentDomain.BaseDirectory + "\\logfile.txt", true);
                sw.WriteLine(message);
                sw.Flush();
                sw.Close();
            }
            catch
            {

            }
        }
    }
}
