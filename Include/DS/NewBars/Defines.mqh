//+------------------------------------------------------------------+
//|                                                      Defines.mqh |
//|                            Copyright 2021, Diamond Systems Corp. |
//|                                       https://diamondsystems.org |
//+------------------------------------------------------------------+
#property copyright "Copyright 2021, Diamond Systems Corp."
#property link      "https://diamondsystems.org"

//+------------------------------------------------------------------+
//| Defines                                                          |
//+------------------------------------------------------------------+
#define NEWBARS_TICK_HISTORY_IN_DAYS   365*3
#define NEWBARS_DB_NAME_PREFIX         "NewBars_"

//+------------------------------------------------------------------+
//| Structure                                                        |
//+------------------------------------------------------------------+
struct NewBar
{
   double open;
   double high;
   double low;
   double close;
   ulong  openMsec;
   ulong  closeMsec;
};

struct NewBarSettings
{
   string symbol;
   string serverName;
   double point;
   double barInPoints;
   int    digits;
};

//+------------------------------------------------------------------+
//| Enums                                                            |
//+------------------------------------------------------------------+
enum ENUM_NEWBARS_SEARCH_TYPE
{
   NEWBARS_SEARCH_POSITION_COUNT,
   NEWBARS_SEARCH_TIME_COUNT,
   NEWBARS_SEARCH_TIME_TIME,
};

enum ENUM_NEWBARS_SEARCH_DIRECTION
{
   NEWBARS_SEARCH_FROM_NEW_TO_OLD,
   NEWBARS_SEARCH_FROM_OLD_TO_NEW,
};

enum ENUM_NEWBARS_SIZE
{
   NEWBARS_SIZE_5      = 5,
   NEWBARS_SIZE_10     = 10,
   NEWBARS_SIZE_50     = 50,
   NEWBARS_SIZE_100    = 100,
   NEWBARS_SIZE_500    = 500,
   NEWBARS_SIZE_1000   = 1000,
   NEWBARS_SIZE_5000   = 5000,
   NEWBARS_SIZE_10000  = 10000,
   NEWBARS_SIZE_50000  = 50000,
   NEWBARS_SIZE_100000 = 100000,
};

enum ENUM_NEWBARS_ERRORS
{
   NEWBARS_ERROR_NONE               = 0,
   NEWBARS_ERROR_DB_NONE            = 1,
   NEWBARS_ERROR_DB_OPEN            = 2,
   NEWBARS_ERROR_DB_CREATE          = 3,
   NEWBARS_ERROR_DB_INDEX           = 4,
   NEWBARS_ERROR_DB_QUERY           = 5,
   NEWBARS_ERROR_DB_TABLE_NOT_FOUND = 6,
   NEWBARS_ERROR_DB_DATA_NOT_FOUND  = 7,
   NEWBARS_ERROR_DB_WRITABLE        = 8,
   NEWBARS_ERROR_DB_TRANSACTION     = 9,
   NEWBARS_ERROR_MQL                = 100,
   NEWBARS_ERROR_USER               = 200,
};
