using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Timers;
using System.Data.SqlClient;
using FComApi;
using Interaction = Microsoft.VisualBasic.Interaction;


namespace FortisToAwdWindowsService
{
    public partial class Scheduler : ServiceBase
    {
        private Timer timer = null;
        #region "Declarations"
        int i;
        bool bResult;
        string str1, str2;
        string strContainer = string.Empty, strSubContainer = string.Empty, strSubContainer1 = string.Empty, strSubContainer2 = string.Empty, Filename=string.Empty;
        string strdetails = string.Empty;
        string Policy = "DECLARE @CNT AS INT,@INTVAL AS INT,@TBLNAME VARCHAR(100),@COLNAME VARCHAR(100),@PARCNT AS INT,@PARINTVAL AS INT"
                            + " DECLARE @CONCNT AS INT,@CONINTVAL AS INT, @CONID AS INT,@CONIDVAL AS INT, @CONID1 AS INT,@CONIDVAL1 AS INT, @CONID2 AS INT,@CONIDVAL2 AS INT"
                            + " DECLARE @SQL VARCHAR(MAX),@SQL1 VARCHAR(MAX),@SQL2 VARCHAR(MAX),@SQL3 VARCHAR(MAX),@SQL4 VARCHAR(MAX),@SQL5 VARCHAR(MAX),@SQL6 VARCHAR(MAX)"
            //+ " DECLARE @SearchString VARCHAR(100),@TBLCOL VARCHAR(100)"
                            + " DECLARE @TBLCOL VARCHAR(100)"
                            + " DECLARE @F_ParentId VARCHAR(100),@F_StorageId VARCHAR(100),@F_Filename VARCHAR(100),@Storage_Path VARCHAR(100),@Parent_ID VARCHAR(100),@Parent_ID1 VARCHAR(100),@Parent_ID2 VARCHAR(100)"
                            + " DECLARE  @TABLEDETAILS TABLE (ID smallint Primary Key IDENTITY(1,1),ColumnName nvarchar(100), TableName nvarchar(100))"
                            + " DECLARE  @POLICYDETAILS TABLE (ID smallint Primary Key IDENTITY(1,1),F_DocumentId INT,F_ParentId nvarchar(100), F_StorageId nvarchar(100),Policy_Number nvarchar(500), F_Filename nvarchar(500))"
                            + " DECLARE  @PATHDETAILS TABLE (ID smallint Primary Key IDENTITY(1,1),Storage_ID nvarchar(100), Storage_Path nvarchar(1000))"
                            + " DECLARE  @CONTAINERDETAILS TABLE (ID smallint Primary Key IDENTITY(1,1),Parent_ID nvarchar(100), Container_ID nvarchar(100),CONTAINER NVARCHAR(200))"
                            + " DECLARE  @CONTAINERDETAILS1 TABLE (ID smallint Primary Key IDENTITY(1,1),Parent_ID nvarchar(100), Container_ID nvarchar(100),CONTAINER NVARCHAR(200))"
                            + " DECLARE  @CONTAINERDETAILS2 TABLE (ID smallint Primary Key IDENTITY(1,1),Parent_ID nvarchar(100), Container_ID nvarchar(100),CONTAINER NVARCHAR(200))"
                            + " DECLARE  @CONTAINERDETAILS3 TABLE (ID smallint Primary Key IDENTITY(1,1),Parent_ID nvarchar(100), Container_ID nvarchar(100),CONTAINER NVARCHAR(200))"
                            + " SET @INTVAL=1 SET @PARINTVAL=1 SET @CONINTVAL=1 SET @CONIDVAL=1 SET @CONIDVAL1=1 SET @CONIDVAL2=1 SET @SQL='' SET @SQL1='' SET @SQL2='' SET @SQL3='' SET @SQL4='' SET @SQL5='' SET @SQL6='' SET @F_Parentid='' "
            //+ " SET @SearchString='A1327B' "
                            + " SELECT   @SQL = @SQL + 'SELECT '''+C.name+''' [Column Name],'''+ T.name + ''' [Table Name] FROM ' + "
                            + " QUOTENAME(SC.name) + '.' + QUOTENAME(T.name) + ' WHERE ' + QUOTENAME(C.name) + ' LIKE ''%' + @SearchString + '%'' HAVING COUNT(*)>0 UNION ALL ' "
                            + " +CHAR(13) + CHAR(10) "
                            + " FROM sys.columns C JOIN sys.tables T  ON C.object_id=T.object_id "
                            + " JOIN     sys.schemas SC ON SC.schema_id=T.schema_id "
                            + " JOIN     sys.types ST ON C.user_type_id=ST.user_type_id "
                            + " JOIN     sys.types SYST ON ST.system_type_id=SYST.user_type_id "
                            + " AND      ST.system_type_id=SYST.system_type_id "
                            + " WHERE    SYST.name IN ('varchar','nvarchar','text','ntext','char','nchar') ORDER BY T.name, C.name "
                            + " IF LEN(@SQL)>12 SELECT @SQL=LEFT(@SQL,LEN(@SQL)- 12) "
                            + " INSERT INTO @TABLEDETAILS EXEC(@SQL)"
                            + " SET @CNT= (SELECT COUNT(TableName) FROM @TABLEDETAILS) IF @CNT IS NOT NULL"
                            + " WHILE @INTVAL<=@CNT  BEGIN"
                            + " SET @TBLNAME =(SELECT TableName FROM @TABLEDETAILS WHERE ID=@INTVAL)"
                            + " SET @COLNAME =(SELECT ColumnName FROM @TABLEDETAILS WHERE ID=@INTVAL)"
                            + " SELECT @SQL1 = @SQL1 + 'SELECT F_DocumentId,F_ParentId,F_StorageId,'+@COLNAME+',F_Filename FROM '"
                            + " +@TBLNAME+ ' WHERE ' + @COLNAME +  ' = '''+  @SearchString + ''''"
                            + " SET @INTVAL+=1 END"
                            + " INSERT INTO @POLICYDETAILS EXEC(@SQL1)"
                            + " SET @PARCNT= (SELECT COUNT(F_Filename) FROM @POLICYDETAILS) IF @PARCNT IS NOT NULL"
                            + " WHILE @PARINTVAL<=@PARCNT BEGIN"
                            + " SET @F_StorageId=(SELECT F_StorageId FROM @POLICYDETAILS WHERE ID=@PARINTVAL)"
                            + " SELECT @SQL2= @SQL2 + 'SELECT Storage_id,Storage_Path FROM FTBStorage WHERE Storage_id = '''+  @F_StorageId + ''''"
                            + " SET @PARINTVAL+=1 END"
                            + " INSERT INTO @PATHDETAILS EXEC (@SQL2)  "
                            + " SET @CONCNT= (SELECT COUNT(F_Filename) FROM @POLICYDETAILS) IF @CONCNT IS NOT NULL"
                            + " WHILE @CONINTVAL<=@CONCNT BEGIN"
                            + " SET @F_ParentId=(SELECT F_ParentId FROM @POLICYDETAILS WHERE ID=@CONINTVAL)"
                            + " SELECT @SQL3= @SQL3 + 'SELECT Parent_ID,Container_ID,Container FROM FTBContainer WHERE Container_ID = '''+  @F_ParentId + ''''"
                            + " SET @CONINTVAL+=1 END"
                            + " INSERT INTO @CONTAINERDETAILS EXEC (@SQL3)  "
                            + " SELECT POL.F_DocumentId,POL.F_ParentId,POL.F_StorageId,POL.F_Filename,POL.Policy_Number,PAT.Storage_Path,CON.Parent_ID as CON_ParentId,CON.Container_ID,CON.Container"
                            + " FROM @POLICYDETAILS POL LEFT JOIN @PATHDETAILS PAT ON POL.ID=PAT.ID LEFT JOIN @CONTAINERDETAILS CON ON POL.ID=CON.ID"
                            + " SET @CONID= (SELECT COUNT(Container_ID) FROM @CONTAINERDETAILS)"
                            + " IF @CONID IS NOT NULL WHILE @CONIDVAL<=@CONID BEGIN"
                            + " SET @Parent_ID=(SELECT Parent_ID FROM @CONTAINERDETAILS WHERE ID=@CONIDVAL)"
                            + " SELECT @SQL4= @SQL4 + 'SELECT Parent_ID,Container_ID,Container FROM FTBContainer WHERE Container_ID = '''+  @Parent_ID + ''''"
                            + " SET @CONIDVAL+=1 END"
                            + " INSERT INTO @CONTAINERDETAILS1 EXEC (@SQL4) SELECT * FROM @CONTAINERDETAILS1"
                            + " SET @CONID1= (SELECT COUNT(Container_ID) FROM @CONTAINERDETAILS1)"
                            + " IF @CONID1 IS NOT NULL WHILE @CONIDVAL1<=@CONID1 BEGIN"
                            + " SET @Parent_ID1=(SELECT Parent_ID FROM @CONTAINERDETAILS1 WHERE ID=@CONIDVAL1)"
                            + " SELECT @SQL5= @SQL5 + 'SELECT Parent_ID,Container_ID,Container FROM FTBContainer WHERE Container_ID = '''+  @Parent_ID1 + ''''"
                            + " SET @CONIDVAL1+=1 END"
                            + " INSERT INTO @CONTAINERDETAILS2 EXEC (@SQL5) SELECT * FROM @CONTAINERDETAILS2"
                            + " SET @CONID2= (SELECT COUNT(Container_ID) FROM @CONTAINERDETAILS2)"
                            + " IF @CONID2 IS NOT NULL WHILE @CONIDVAL2<=@CONID2 BEGIN"
                            + " SELECT @SQL6= @SQL6 + 'SELECT Parent_ID,Container_ID,Container FROM FTBContainer WHERE Container_ID = '''+  @Parent_ID2 + ''''"
                            + " SET @CONIDVAL2+=1 END"
                            + " INSERT INTO @CONTAINERDETAILS3 EXEC (@SQL6) SELECT * FROM @CONTAINERDETAILS3 ";
        FComApi.Application app;
        FComApi.Database db;
        DocFile MAGfile;
        string dbname;
        string Container_ID = string.Empty;
        DataTable dt = new DataTable();
        DataTable dt1 = new DataTable();
        DataTable dt2 = new DataTable();
        #endregion

        public Scheduler()
        {
           InitializeComponent();
            Conversion();
        }
        //public void onDebug()
        //{
        //    OnStart(null);
        //}
        private void InitializeComponent()
        {
            this.timer = new System.Timers.Timer();
            ((System.ComponentModel.ISupportInitialize)(this.timer)).BeginInit();
            // 
            // timer
            // 
            this.timer.Enabled = true;
            this.timer.Interval = 10000D;
            this.timer.Elapsed += new System.Timers.ElapsedEventHandler(this.timerStow_Elapsed);
            // 
            // Scheduler
            // 
            this.ServiceName = "Service1";
            ((System.ComponentModel.ISupportInitialize)(this.timer)).EndInit();

        }

        
        protected override void OnStart(string[] args)
        {
            timer = new Timer();
            timer.Interval = 10000;
            timer.Enabled = true;
          //  Conversion();
            LogClass.WriteLog("Service started" + DateTime.Now.ToString("HH:mm:ss tt").ToString());

        }

        protected override void OnStop()
        {
            timer.Enabled = false;
            LogClass.WriteLog("Service stopped" + DateTime.Now.ToString("HH:mm:ss tt").ToString());
        }

        public bool PolicyDetails()
        {
            using (SqlConnection conn = new SqlConnection(@"Data Source=PW7AM1XDH000552; Initial Catalog=Fortis;User ID=sa;Password=Welcome123"))
            {
                try
                {
                    SqlCommand cmd1 = new SqlCommand("Select * from tblContainer ", conn);
                    SqlCommand cmd2 = new SqlCommand("Select * from tblContainer_Indexdata", conn);
                    conn.Open();
                    cmd1.ExecuteNonQuery();
                    cmd2.ExecuteNonQuery();
                    SqlDataAdapter da1 = new SqlDataAdapter(cmd1);
                    SqlDataAdapter da2 = new SqlDataAdapter(cmd2);
                    da1.Fill(dt1);
                    da2.Fill(dt2);
                    LogClass.WriteLog("sqlconnection success");
                    return true;
                }
                catch (Exception e)
                {
                    LogClass.WriteLog(e.Message);
                    return false;
                }
            }
            
        }
        private void timerStow_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            timer.Enabled = false;

           // Conversion();

            timer.Enabled = true;
        }

        private void Conversion()
        {            
            bResult = PolicyDetails();
            if (bResult == true)
            {
                try
                {                   
                    object obj = Interaction.CreateObject("Fortis.Application");
                    app = (FComApi.Application)obj;
                    app.Login("sysadm", "G3t0utN0w", FaStationType.faScan);

                    long lDBCount;
                    lDBCount = app.Databases.Count;

                    if (lDBCount >= 1)
                    {
                        db = app.Databases(1);
                        dbname = db.Name;
                        string strfn = "A1914";
                        LogClass.WriteLog("Policy Number: " + strfn);
                        SqlConnection con = new SqlConnection("Data Source=pwsadsfortis01;Initial Catalog= " + dbname + ";Integrated Security=True;");
                        SqlCommand cmd;
                        dt = new DataTable();
                        SqlDataAdapter da;
                        DataSet ds = new DataSet();
                        cmd = new SqlCommand(Policy, con);
                        cmd.CommandType = System.Data.CommandType.Text;
                        cmd.Parameters.Add("@SearchString", SqlDbType.VarChar);
                        cmd.Parameters["@SearchString"].Value = strfn;
                        con.Open();
                        cmd.ExecuteNonQuery();
                        da = new SqlDataAdapter(cmd);
                        da.Fill(ds);
                        da.Fill(dt);
                        dt.Columns.Add("P_container", typeof(System.String));
                        i = 0;
                        LogClass.WriteLog("---------------------------------------------------------------------------------------");

                        foreach (DataRow row in dt.Rows)
                        {
                            //need to set value to NewColumn column
                            row["P_container"] = ds.Tables[1].Rows[i]["Container"].ToString().Trim();   // or set it to some other value
                            i++;
                        }

                        var Grpcontainer = from table in dt.AsEnumerable()
                                           group table by new { placeCol = table["Container"] } into groupby
                                           select new
                                           {
                                               Value = groupby.Key,
                                               ColumnValues = groupby
                                           };

                        // LogClass.WriteLog("******checking containers one by one*******");


                        foreach (var Key in Grpcontainer)
                        {

                            strSubContainer = Key.Value.placeCol.ToString();
                            if (strSubContainer != "PARTICIPANT INFO" && strSubContainer != "PENSIONS")
                            {
                                foreach (var columnValue in Key.ColumnValues)
                                {
                                    string Doc_id = columnValue["F_DocumentId"].ToString();
                                    string container = columnValue["container"].ToString();
                                    Filename = columnValue["F_Filename"].ToString();
                                    strContainer = columnValue["P_container"].ToString();
                                    DataRow[] dr = dt1.Select("Container_Name = '" + container + "'");
                                    if (dr.Length > 0)
                                    {
                                        Container_ID = dr[0]["Container_ID"].ToString();
                                    }
                                    DataRow[] dr1 = dt2.Select("Container_ID =" + Container_ID);
                                    DataTable dtcontainer = new DataTable();
                                    dtcontainer = dr1.CopyToDataTable();
                                    DataRow[] dr2 = dtcontainer.Select("F_DocumentID =" + Doc_id);
                                    if (dr2.Length > 0)
                                    {
                                        i = Convert.ToInt32(dr2[0]["Container_Index_Number"]);
                                    }
                                    if (strSubContainer != strContainer)
                                    {
                                        str1 = db.RootFolder.Folders(strContainer).Folders(strSubContainer).Name;
                                    }
                                    else
                                    {
                                        str1 = "";
                                    }

                                    str2 = db.RootFolder.Folders(strContainer).Name;

                                    if (str1 == strSubContainer)
                                    {
                                        LogClass.WriteLog("Container :" + strSubContainer);
                                        LogClass.WriteLog("Starting Time: " + DateTime.Now.ToString("HH:mm:ss tt").ToString());

                                        int Docid = db.RootFolder.Folders(strContainer).Folders(strSubContainer).Documents.Item(i).ID;
                                        if (Doc_id == Docid.ToString())
                                        {
                                            MAGfile = db.RootFolder.Folders(strContainer).Folders(strSubContainer).Documents.Item(i).DocFile;
                                            LogClass.WriteLog("DOC Found in the place of " + i + ". looping Time to reach the place - " + DateTime.Now.ToString("HH:mm:ss tt").ToString());
                                            // MAGfile.Export("C:\\Code\\" + Filename, FaExportDocFileType.faTIFFile, 22651, 22651);
                                        }
                                        else
                                        {
                                            LogClass.WriteLog("Index MissMatch Docid- " + Doc_id + " - " + DateTime.Now.ToString("HH:mm:ss tt").ToString());
                                        }

                                        LogClass.WriteLog("Ending Time: " + DateTime.Now.ToString("HH:mm:ss tt").ToString());
                                        LogClass.WriteLog("-----------------------------------------------------------------------------------------");

                                    }
                                    else
                                        if (str2 == strSubContainer)
                                        {

                                            //var objj1 = db.RootFolder.Folders(strContainer).Documents();
                                            LogClass.WriteLog("Container :" + strContainer);
                                            LogClass.WriteLog("Starting Time: " + DateTime.Now.ToString("HH:mm:ss tt").ToString());

                                            int Docid = db.RootFolder.Folders(strContainer).Documents.Item(i).ID;
                                            if (Doc_id == Docid.ToString())
                                            {
                                                MAGfile = db.RootFolder.Folders(strContainer).Documents.Item(i).DocFile;
                                                LogClass.WriteLog("DOC Found in the place of " + i + ". looping Time to reach the place - " + DateTime.Now.ToString("HH:mm:ss tt").ToString());
                                                // MAGfile.Export("C:\\Code\\" + Filename, FaExportDocFileType.faTIFFile, 22651, 22651);
                                            }
                                            else
                                            {
                                                LogClass.WriteLog("Index MissMatch Docid- " + Doc_id + " - Index number - " + i + " - " + DateTime.Now.ToString("HH:mm:ss tt").ToString());
                                            }

                                            LogClass.WriteLog("Ending Time: " + DateTime.Now.ToString("HH:mm:ss tt").ToString());
                                            LogClass.WriteLog("-----------------------------------------------------------------------------------------");
                                        }
                                }
                            }
                        }
                    }
                    LogClass.WriteLog("App End Time: " + DateTime.Now.ToString("HH:mm:ss tt").ToString());
                }
                catch (Exception ex)
                {
                    LogClass.WriteLog(ex.Message);
                }
            }
        }
    }
}
