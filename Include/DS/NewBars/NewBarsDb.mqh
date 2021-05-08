//+------------------------------------------------------------------+
//|                                                    NewBarsDb.mqh |
//|                            Copyright 2021, Diamond Systems Corp. |
//|                                       https://diamondsystems.org |
//+------------------------------------------------------------------+
#property copyright "Copyright 2021, Diamond Systems Corp."
#property link      "https://diamondsystems.org"
#property version   "0.01"

#include "Defines.mqh";

//+------------------------------------------------------------------+
//| New bars DB                                                      |
//+------------------------------------------------------------------+
class NewBarsDb
{
   private:
      string         m_DbName;
      int            m_DbH;
      int            m_TickHistoryInSec;
      bool           m_IsWritable;
      NewBarSettings m_Settings;
      NewBarSettings m_DefaultSettings;
      string         m_PointFormat;
      //---
      ENUM_NEWBARS_ERRORS m_ErrorCode;
      string              m_ErrorMsg;
      //---
      bool           CreateDbTableSettings();
      bool           CreateDbTableBars();
      bool           UpdateSettings();
   
   protected:
      virtual string GetDbNamePrefix()   { return NEWBARS_DB_NAME_PREFIX; }
      virtual int    GetNewBarsByTemplate(
                        const int                      dbH,
                        const string                   tableName,
                        ENUM_NEWBARS_SEARCH_TYPE       searchType,
                        const ulong                    startIndex,
                        const ulong                    stopIndex,
                        NewBar                        &newBars[],
                        ENUM_NEWBARS_SEARCH_DIRECTION  searchDirection
                     );
      void           SetError(const ENUM_NEWBARS_ERRORS errorCode, const string errorMsg);
      void           SetErrorDbClosed();
   
   public:
      void           NewBarsDb();
      void          ~NewBarsDb();
      //---
      bool           OpenDb(
                        const bool              isWritable = false,
                        const string            dbName = "",
                        const ENUM_NEWBARS_SIZE newBarSize = NEWBARS_SIZE_50,
                        const int               tickHistoryInDays = NEWBARS_TICK_HISTORY_IN_DAYS
                     );
      void           CloseDb();
      //---
      void           ResetError();
      void           SetDefaultSettings(const string symbol, const int digits, const double point, const string serverName);
      //---
      bool           IsOpenDb()          { return m_DbH != NULL; }
      bool           IsWritable()        { return m_IsWritable;  }
      int            GetDbHandle()       { return m_DbH;         }
      string         GetDbName()         { return m_DbName;      }
      NewBarSettings GetSettings()       { return m_Settings;    }
      ENUM_NEWBARS_ERRORS GetErrorCode() { return m_ErrorCode;   }
      string              GetErrorMsg()  { return m_ErrorMsg;    }
      virtual ulong  GetLastTimeMsec();
      virtual ulong  GetStartHistoryTimeMsc();
      //---
      int            NewBarsCount();
      int            GetNewBar(NewBar &newBar, ENUM_NEWBARS_SEARCH_DIRECTION searchDirection = NEWBARS_SEARCH_FROM_NEW_TO_OLD);
      int            GetNewBars(NewBar &newBars[], const int startPosition = 1, const int count = 1000, ENUM_NEWBARS_SEARCH_DIRECTION searchDirection = NEWBARS_SEARCH_FROM_NEW_TO_OLD);
      int            GetNewBars(NewBar &newBars[], const datetime startTime, const int count = 1000, ENUM_NEWBARS_SEARCH_DIRECTION searchDirection = NEWBARS_SEARCH_FROM_NEW_TO_OLD);
      int            GetNewBars(NewBar &newBars[], const datetime startTime, const datetime stopTime, ENUM_NEWBARS_SEARCH_DIRECTION searchDirection = NEWBARS_SEARCH_FROM_NEW_TO_OLD);
      //---
      bool           AddNewBar(const double open, const double high, const double low, const double close, const ulong openMsec, const ulong closeMsec);
      bool           AddNewBar(const NewBar &newBar);
      bool           AddNewBars(const NewBar &newBars[], const int start = 0, const int count = WHOLE_ARRAY);
};

//+------------------------------------------------------------------+
//| Constructor                                                      |
//+------------------------------------------------------------------+
void NewBarsDb::NewBarsDb(): m_IsWritable(false)
{}

//+------------------------------------------------------------------+
//| Destructor                                                       |
//+------------------------------------------------------------------+
void NewBarsDb::~NewBarsDb()
{
   CloseDb();
}

//+------------------------------------------------------------------+
//| Set default settings                                             |
//+------------------------------------------------------------------+
void NewBarsDb::SetDefaultSettings(const string symbol, const int digits, const double point, const string serverName)
{
   m_DefaultSettings.symbol      = symbol;
   m_DefaultSettings.digits      = digits;
   m_DefaultSettings.point       = point;
   m_DefaultSettings.serverName  = serverName;
}

//+------------------------------------------------------------------+
//| Open DB                                                          |
//+------------------------------------------------------------------+
bool NewBarsDb::OpenDb(const bool isWritable, const string dbName, const ENUM_NEWBARS_SIZE newBarSize, const int tickHistoryInDays)
{
   ResetError();
   
   if (IsOpenDb())
      CloseDb();
   
   //--- environment initialization
   //-------------------------------
   if (m_DefaultSettings.symbol == NULL)
      SetDefaultSettings(_Symbol, _Digits, _Point, AccountInfoString(ACCOUNT_SERVER));
   m_DefaultSettings.barInPoints = newBarSize * _Point;
   
   m_IsWritable  = isWritable;
   m_PointFormat = StringFormat("%%.%df", m_DefaultSettings.digits);
   
   if (dbName == NULL || StringLen(dbName) == 0)
      m_DbName = GetDbNamePrefix()+ m_DefaultSettings.symbol +"_S"+ string(newBarSize) +"_"+ m_DefaultSettings.serverName +".db";
   else
      m_DbName = dbName;
   
   m_TickHistoryInSec = 24*60*60;
   if (tickHistoryInDays > NEWBARS_TICK_HISTORY_IN_DAYS)
      m_TickHistoryInSec *= NEWBARS_TICK_HISTORY_IN_DAYS;
   else if (tickHistoryInDays > 0)
      m_TickHistoryInSec *= tickHistoryInDays;
   
   //--- DB and settings
   //--------------------
   if (m_IsWritable)
      m_DbH = DatabaseOpen(m_DbName, DATABASE_OPEN_READWRITE | DATABASE_OPEN_CREATE);
   else
      m_DbH = DatabaseOpen(m_DbName, DATABASE_OPEN_READONLY);
   if (m_DbH == INVALID_HANDLE)
   {
      m_DbH = NULL;
      SetError(NEWBARS_ERROR_DB_OPEN, "open/create failed");
      return false;
   }
   
   DatabaseExecute(m_DbH, "PRAGMA journal_mode = DELETE;");
   
   if (CreateDbTableSettings() && CreateDbTableBars() && UpdateSettings())
      return true;
   
   CloseDb();
   return false;
}

//+------------------------------------------------------------------+
//| Close DB                                                         |
//+------------------------------------------------------------------+
void NewBarsDb::CloseDb()
{
   if (m_DbH == NULL)
      return;
   DatabaseClose(m_DbH);
   m_DbH = NULL;
}

//+------------------------------------------------------------------+
//| Create DB table - settings                                       |
//+------------------------------------------------------------------+
bool NewBarsDb::CreateDbTableSettings()
{
   ResetLastError();
   if (DatabaseTableExists(m_DbH, "settings"))
      return true;
   
   if (GetLastError() != 5126 || ! m_IsWritable)
   {
      SetError(NEWBARS_ERROR_DB_TABLE_NOT_FOUND, "table 'settings' not found");
      return false;
   }
   
   if (! DatabaseExecute(m_DbH, "CREATE TABLE settings ("
                                   "id            INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
                                   "symbol        STRING  (32)  NOT NULL,"
                                   "digits        INTEGER       NOT NULL,"
                                   "point         DOUBLE        NOT NULL,"
                                   "bar_in_points DOUBLE        NOT NULL,"
                                   "server_name   STRING  (256) NOT NULL"
                                ");"))
   {
      SetError(NEWBARS_ERROR_DB_CREATE, "create 'settings' table failed");
      return false;
   }
   
   return true;
}

//+------------------------------------------------------------------+
//| Create DB table - bars                                           |
//+------------------------------------------------------------------+
bool NewBarsDb::CreateDbTableBars()
{
   ResetLastError();
   if (DatabaseTableExists(m_DbH, "bars"))
      return true;
   
   if (GetLastError() != 5126 || ! m_IsWritable)
   {
      SetError(NEWBARS_ERROR_DB_TABLE_NOT_FOUND, "table 'bars' not found");
      return false;
   }
   
   if (! DatabaseExecute(m_DbH, "CREATE TABLE bars ("
                                   "id         INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
                                   "open       DOUBLE  NOT NULL,"
                                   "high       DOUBLE  NOT NULL,"
                                   "low        DOUBLE  NOT NULL,"
                                   "close      DOUBLE  NOT NULL,"
                                   "open_msec  BIGINT  NOT NULL,"
                                   "close_msec BIGINT  NOT NULL"
                                ");"))
   {
      SetError(NEWBARS_ERROR_DB_CREATE, "create 'bars' table failed");
      return false;
   }
   
   if (! DatabaseExecute(m_DbH, "CREATE INDEX OpenMsec ON bars (open_msec);"))
   {
      SetError(NEWBARS_ERROR_DB_INDEX, "create index failed");
      return false;
   }
   
   if (! DatabaseExecute(m_DbH, "CREATE INDEX CloseMsec ON bars (close_msec);"))
   {
      SetError(NEWBARS_ERROR_DB_INDEX, "create index failed");
      return false;
   }
   
   return true;
}

//+------------------------------------------------------------------+
//| Update settings                                                  |
//+------------------------------------------------------------------+
bool NewBarsDb::UpdateSettings()
{
   int dbR = DatabasePrepare(m_DbH, "SELECT symbol, server_name, point, bar_in_points, digits FROM settings ORDER BY id DESC LIMIT 1");
   if (dbR == INVALID_HANDLE)
   {
      SetError(NEWBARS_ERROR_DB_QUERY, "request 'get settings' failed");
      return false;
   }
   
   bool result = DatabaseReadBind(dbR, m_Settings);
   DatabaseFinalize(dbR);
   
   bool isClearSettings = false;
   if (result)
   {
      if (m_Settings.symbol != ""  && m_Settings.serverName  != ""  &&
          m_Settings.point  != 0.0 && m_Settings.barInPoints != 0.0 &&
          m_Settings.digits != 0)
      { return true; }
   }
   
   if (! m_IsWritable)
   {
      SetError(NEWBARS_ERROR_DB_DATA_NOT_FOUND, "'settings' not found");
      return false;
   }
   
   //--- if a failed settings
   if (result && ! DatabaseExecute(m_DbH, "DELETE FROM settings;"))
   {
      SetError(NEWBARS_ERROR_DB_QUERY, "request 'clear settings' failed");
      return false;
   }
   
   string sql = StringFormat("INSERT INTO settings (symbol, server_name, point, bar_in_points, digits)"
                                "VALUES ('%s', '%s', "+m_PointFormat+", "+m_PointFormat+", %d)",
                             m_DefaultSettings.symbol,
                             m_DefaultSettings.serverName,
                             m_DefaultSettings.point,
                             m_DefaultSettings.barInPoints,
                             m_DefaultSettings.digits);
   if (! DatabaseExecute(m_DbH, sql))
   {
      SetError(NEWBARS_ERROR_DB_QUERY, "insert 'settings' failed");
      return false;
   }
   
   m_Settings = m_DefaultSettings;
   return true;
}

//+------------------------------------------------------------------+
//| Get start history time in milliseconds                           |
//+------------------------------------------------------------------+
ulong NewBarsDb::GetStartHistoryTimeMsc()
{
   return (TimeCurrent() - m_TickHistoryInSec) * 1000;
}

//+------------------------------------------------------------------+
//| Get the last known time in miliseconds                           |
//+------------------------------------------------------------------+
ulong NewBarsDb::GetLastTimeMsec()
{
   if (m_DbH == NULL)
   {
      SetErrorDbClosed();
      return 0;
   }
   
   int dbR = DatabasePrepare(m_DbH, "SELECT close_msec FROM bars ORDER BY id DESC LIMIT 1");
   if (dbR == INVALID_HANDLE)
   {
      SetError(NEWBARS_ERROR_DB_QUERY, "request 'get the last known time in miliseconds' failed");
      return 0;
   }
   
   ulong timeMsec = 0;
   if (DatabaseRead(dbR))
      DatabaseColumnLong(dbR, 0, timeMsec);
   DatabaseFinalize(dbR);
   
   if (timeMsec == 0)
      SetError(NEWBARS_ERROR_DB_NONE, "");
   
   return timeMsec;
}

//+------------------------------------------------------------------+
//| NewBars cont                                                     |
//+------------------------------------------------------------------+
int NewBarsDb::NewBarsCount()
{
   if (m_DbH == NULL)
   {
      SetErrorDbClosed();
      return 0;
   }
   
   int dbR = DatabasePrepare(m_DbH, "SELECT COUNT(id) FROM bars");
   if (dbR == INVALID_HANDLE)
   {
      SetError(NEWBARS_ERROR_DB_QUERY, "request 'NewBars count' failed");
      return 0;
   }
   
   int result;
   if (! DatabaseRead(dbR) || ! DatabaseColumnInteger(dbR, 0, result))
      result = 0;
   DatabaseFinalize(dbR);
   
   return result;
}

//+------------------------------------------------------------------+
//| Get NewBar                                                       |
//| ---------------------                                            |
//| return:                                                          |
//| <= 0 = NewBars count                                             |
//| ==-1 = false                                                     |
//+------------------------------------------------------------------+
int NewBarsDb::GetNewBar(NewBar &newBar, ENUM_NEWBARS_SEARCH_DIRECTION searchDirection)
{
   if (m_DbH == NULL)
   {
      SetErrorDbClosed();
      return -1;
   }
   
   string sql = "SELECT open, high, low, close, open_msec, close_msec FROM bars";
   if (searchDirection == NEWBARS_SEARCH_FROM_NEW_TO_OLD)
      sql += " ORDER BY id DESC";
   sql += " LIMIT 1";
   
   int dbR = DatabasePrepare(m_DbH, sql);
   if (dbR == INVALID_HANDLE)
   {
      SetError(NEWBARS_ERROR_DB_QUERY, "request 'get NewBars' failed");
      return -1;
   }
   
   ResetLastError();
   int result = 1;
   if (! DatabaseReadBind(dbR, newBar))
   {
      result = (GetLastError() != 5126) ? -1 : 0;
      SetError(NEWBARS_ERROR_DB_DATA_NOT_FOUND, "NewBars not found");
   }
   
   DatabaseFinalize(dbR);
   return result;
}

//+------------------------------------------------------------------+
//| Get NewBars by Limit                                             |
//| ---------------------                                            |
//| return:                                                          |
//| <= 0 NewBars count                                               |
//| ==-1 false                                                       |
//+------------------------------------------------------------------+
int NewBarsDb::GetNewBars(NewBar &newBars[], const int startPosition, const int count, ENUM_NEWBARS_SEARCH_DIRECTION searchDirection)
{
   if (startPosition < 1 || count < 0)
      return 0;
   
   return GetNewBarsByTemplate(m_DbH, "bars", NEWBARS_SEARCH_POSITION_COUNT, startPosition, count, newBars, searchDirection);
}

//+------------------------------------------------------------------+
//| Get NewBars by Time                                              |
//| --------------------                                             |
//| return:                                                          |
//| <= 0 NewBars count                                               |
//| ==-1 false                                                       |
//+------------------------------------------------------------------+
int NewBarsDb::GetNewBars(NewBar &newBars[], const datetime startTime, const int count, ENUM_NEWBARS_SEARCH_DIRECTION searchDirection)
{
   if (startTime < 1 || count < 0)
      return 0;
   
   return GetNewBarsByTemplate(m_DbH, "bars", NEWBARS_SEARCH_TIME_COUNT, (startTime * 1000), count, newBars, searchDirection);
}

//+------------------------------------------------------------------+
//| Get NewBars by Time                                              |
//| --------------------                                             |
//| return:                                                          |
//| <= 0 NewBars count                                               |
//| ==-1 false                                                       |
//+------------------------------------------------------------------+
int NewBarsDb::GetNewBars(NewBar &newBars[], const datetime startTime, const datetime stopTime, ENUM_NEWBARS_SEARCH_DIRECTION searchDirection)
{
   if (startTime < 1 || stopTime < 1)
      return 0;
   
   return GetNewBarsByTemplate(m_DbH, "bars", NEWBARS_SEARCH_TIME_TIME, (startTime * 1000), (stopTime * 1000), newBars, searchDirection);
}

//+------------------------------------------------------------------+
//| Get NewBars by templates                                         |
//| -------------------------                                        |
//| return:                                                          |
//| <= 0 NewBars count                                               |
//| ==-1 false                                                       |
//+------------------------------------------------------------------+
int NewBarsDb::GetNewBarsByTemplate(
      const int dbH,
      const string tableName,
      ENUM_NEWBARS_SEARCH_TYPE searchType,
      const ulong startIndex,
      const ulong stopIndex,
      NewBar &newBars[],
      ENUM_NEWBARS_SEARCH_DIRECTION searchDirection)
{
   if (dbH == NULL)
   {
      SetErrorDbClosed();
      return -1;
   }
   
   if (! ArrayIsDynamic(newBars))
   {
      SetError(NEWBARS_ERROR_USER, "array NewBars is not dynamic");
      return -1;
   }
   
   string sql = "SELECT open, high, low, close, open_msec, close_msec FROM "+tableName;
   
   switch (searchType)
   {
      case NEWBARS_SEARCH_POSITION_COUNT:
      {
         if (searchDirection == NEWBARS_SEARCH_FROM_NEW_TO_OLD)
            sql += " ORDER BY id DESC";
         sql += " LIMIT "+string(stopIndex > 0 ? stopIndex : INT_MAX);
         if (startIndex-1 > 0)
            sql += " OFFSET "+string(startIndex-1);
         break;
      }
      
      case NEWBARS_SEARCH_TIME_COUNT:
      {
         if (searchDirection == NEWBARS_SEARCH_FROM_NEW_TO_OLD)
         {
            sql += " WHERE open_msec <= "+string(startIndex);
            sql += " ORDER BY id DESC";
         }
         else
            sql += " WHERE open_msec >= "+string(startIndex);
         sql += " LIMIT "+string(stopIndex > 0 ? stopIndex : INT_MAX);
         break;
      }
      
      case NEWBARS_SEARCH_TIME_TIME:
      {
         if (searchDirection == NEWBARS_SEARCH_FROM_NEW_TO_OLD)
         {
            if (startIndex <= stopIndex)
               return 0;
            sql += " WHERE open_msec <= "+string(startIndex)+" AND open_msec >= "+string(stopIndex);
         }
         else
         {
            if (startIndex >= stopIndex)
               return 0;
            sql += " WHERE open_msec >= "+string(startIndex)+" AND open_msec <= "+string(stopIndex);
         }
         break;
      }
   }
   
   int dbR = DatabasePrepare(dbH, sql);
   if (dbR == INVALID_HANDLE)
   {
      SetError(NEWBARS_ERROR_DB_QUERY, "request 'get NewBars' failed");
      return -1;
   }
   
   ArrayFree(newBars);
   NewBar newBar;
   int i = 0;
   
   for (; DatabaseReadBind(dbR, newBar); i++)
   {
      if (! ArrayResize(newBars, i+1, 100))
      {
         ArrayFree(newBars);
         SetError(NEWBARS_ERROR_MQL, "ArrayResize(NewBars, "+string(i+1)+", 100)' failed");
         i = -1;
         break;
      }
      newBars[i] = newBar;
   }
   DatabaseFinalize(dbR);
   
   return i;
}

//+------------------------------------------------------------------+
//| Add new bar by args                                              |
//+------------------------------------------------------------------+
bool NewBarsDb::AddNewBar(const double open, const double high, const double low, const double close, const ulong openMsec, const ulong closeMsec)
{
   NewBar newBars[1];
   //---
   newBars[0].open      = open;
   newBars[0].high      = high;
   newBars[0].low       = low;
   newBars[0].close     = close;
   newBars[0].openMsec  = openMsec;
   newBars[0].closeMsec = closeMsec;
   
   return AddNewBars(newBars);
}

//+------------------------------------------------------------------+
//| Add new bar by struct                                            |
//+------------------------------------------------------------------+
bool NewBarsDb::AddNewBar(const NewBar &newBar)
{
   NewBar newBars[1];
   newBars[0] = newBar;
   return AddNewBars(newBars);
}

//+------------------------------------------------------------------+
//| Add new bars                                                     |
//+------------------------------------------------------------------+
bool NewBarsDb::AddNewBars(const NewBar &newBars[], const int start, const int count)
{
   if (m_DbH == NULL)
   {
      SetErrorDbClosed();
      return false;
   }
   
   if (! m_IsWritable)
   {
      SetError(NEWBARS_ERROR_DB_WRITABLE, "it is forbidden to add NewBar");
      return false;
   }
   
   if (start < 0)
   {
      SetError(NEWBARS_ERROR_USER, "parameter 'start'="+string(start));
      return false;
   }
   
   int arrSize = ArraySize(newBars);
   if (arrSize == 0 || count == 0)
      return true;
   if (count > 0 && count < arrSize)
      arrSize = count;
   if (arrSize <= start)
      return true;
   
   if (! DatabaseTransactionBegin(m_DbH))
   {
      SetError(NEWBARS_ERROR_DB_TRANSACTION, "transaction begin failed");
      return false;
   }
   
   string sql;
   NewBar newBar;
   
   for (int i=start; i<arrSize; i++)
   {
      newBar = newBars[i];
      sql = StringFormat("INSERT INTO bars (open, high, low, close, open_msec, close_msec)"
                            "VALUES ("+m_PointFormat+", "+m_PointFormat+", "+m_PointFormat+", "+m_PointFormat+", %I64u, %I64u)",
                         newBar.open,
                         newBar.high,
                         newBar.low,
                         newBar.close,
                         newBar.openMsec,
                         newBar.closeMsec);
      if (! DatabaseExecute(m_DbH, sql))
      {
         SetError(NEWBARS_ERROR_DB_QUERY, "insert 'NewBar' failed");
         DatabaseTransactionRollback(m_DbH);
         return false;
      }
   }
   
   if (! DatabaseTransactionCommit(m_DbH))
   {
      SetError(NEWBARS_ERROR_DB_TRANSACTION, "transaction commit failed");
      DatabaseTransactionRollback(m_DbH);
      return false;
   }
   
   return true;
}

//+------------------------------------------------------------------+
//| Set error message                                                |
//+------------------------------------------------------------------+
void NewBarsDb::SetError(const ENUM_NEWBARS_ERRORS errorCode, const string errorMsg)
{
   m_ErrorCode = errorCode;
   
   if (m_ErrorCode < 100)
      m_ErrorMsg = "Error DB: "+ m_DbName +" "+ errorMsg +". Code="+(string)GetLastError();
   else if (m_ErrorCode < 200)
      m_ErrorMsg = "Error: "+ errorMsg +". Code="+(string)GetLastError();
   else
      m_ErrorMsg = "Error: "+ errorMsg;
}

//+------------------------------------------------------------------+
//| Set error message - DB closed                                    |
//+------------------------------------------------------------------+
void NewBarsDb::SetErrorDbClosed()
{
   SetError(NEWBARS_ERROR_USER, "DB closed :)");
}

//+------------------------------------------------------------------+
//| Reset error                                                      |
//+------------------------------------------------------------------+
void NewBarsDb::ResetError()
{
   m_ErrorCode = NEWBARS_ERROR_NONE;
   m_ErrorMsg  = "";
}

//+------------------------------------------------------------------+
