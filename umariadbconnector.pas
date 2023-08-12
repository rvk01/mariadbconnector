unit umariadbconnector;

{$mode ObjFPC}{$H+}
{.$WARN 5091 off : Local variable "$1" of a managed type does not seem to be initialized}

interface

uses
  SysUtils, blcksock, synacode,
  strutils, DB, BufDataset,
  typinfo, DateUtils, Math;

const
  MariaDbDebug: boolean = False;

type
  MySqlCommands = (
    { $01 } COMMAND_SLEEP,         // Not used currently. */
    { $02 } COMMAND_QUIT,
    { $03 } COMMAND_INIT_DB,
    { $04 } COMMAND_QUERY,
    { $05 } COMMAND_MYSQL_LIST,    // Deprecated. */
    { $06 } COMMAND_CREATE_DB,     // Deprecated. */
    { $07 } COMMAND_DROP_DB,       // Deprecated. */
    { $08 } COMMAND_REFRESH,
    { $09 } COMMAND_SHUTDOWN,
    { $0A } COMMAND_STATISTICS,
    { $0B } COMMAND_PROCESS_INFO,    // Deprecated. */
    { $0C } COMMAND_CONNECT,
    { $0D } COMMAND_PROCESS_KILL,    // Deprecated. */
    { $0E } COMMAND_DEBUG,
    { $0F } COMMAND_PING,
    { $10 } COMMAND_TIME,
    { $11 } COMMAND_DELAYED_INSERT,
    { $12 } COMMAND_CHANGE_USER,
    { $13 } COMMAND_BINLOG_DUMP,
    { $14 } COMMAND_TABLE_DUMP,
    { $15 } COMMAND_CONNECT_OUT,
    { $16 } COMMAND_REGISTER_SLAVE,
    { $17 } COMMAND_STMT_PREPARE,
    { $18 } COMMAND_STMT_EXECUTE,
    { $19 } COMMAND_STMT_SEND_LONG_DATA,
    { $1A } COMMAND_STMT_CLOSE,
    { $1B } COMMAND_STMT_RESET,
    { $1C } COMMAND_SET_OPTION,
    { $1D } COMMAND_STMT_FETCH,
    { $1E } COMMAND_DAEMON,
    { $1F } COMMAND_END
    );

type
  TMariaDBConnector = class(TObject)
    Sock: TTCPBlockSocket;
    FLastError: integer;
    FLastErrorDesc: string;
    FPackNumber: byte;
    FDataset: TBufDataset;
    FRowsAffected: integer;
    FLastInsertId: integer;
    FServerStatus: integer;
    FWarningCount: integer;
  private
    procedure DebugStr(Log: string);
    function _set_int(Value: uint64; Len: Integer): rawbytestring;
    function _get_int(Buffer: rawbytestring; var Ps: uint32; Len: integer = -1): integer; // -1 = lenenc
    function _get_str(Buffer: rawbytestring; var Ps: uint32; Len: integer = -1): rawbytestring;
    function _is_error(Buffer: rawbytestring): boolean;
    function _is_ok(Buffer: rawbytestring): boolean;
    function _is_eof(Buffer: rawbytestring): boolean;
  public
    constructor Create;
    destructor Destroy; override;
    procedure SendPacket(Buffer: rawbytestring);
    function ReceivePacket(Timeout: integer): rawbytestring;
    function ConnectAndLogin(AServer, APort, AUser, APassword, ADatabase: rawbytestring): boolean;
    function ExecuteCommand(Command: MySqlCommands; SQL: rawbytestring = ''): boolean;
    function Query(SQL: string): boolean;
    function Ping: boolean;
    procedure SetMultiOptions(Value: boolean);
    procedure Quit;
    property LastError: integer read FLastError;
    property LastErrorDesc: string read FLastErrorDesc;
    property Dataset: TBufDataset read FDataset;
  end;


implementation

// for debugging to screen as hex
function Buf2Hex(Buffer: rawbytestring): rawbytestring;
var
  I: char;
begin
  Result := '';
  for I in Buffer do Result := Result + ' ' + HexStr(byte(I), 2);
end;

const
  // flags for field types
  NOT_NULL_FLAG = 1;            //  Field can't be NULL
  PRIMARY_KEY_FLAG = 2;         //  Field is part of a primary key
  UNIQUE_KEY_FLAG = 4;          //  Field is part of a unique key
  MULTIPLE_KEY_FLAG = 8;        //  Field is part of a key
  BLOB_FLAG = 16;               //  Field is a blob
  UNSIGNED_FLAG = 32;           //  Field is unsigned
  ZEROFILL_FLAG = 64;           //  Field is zerofill
  BINARY_FLAG = 128;            //  Field is binary
  ENUM_FLAG = 256;              // field is an enum
  AUTO_INCREMENT_FLAG = 512;    // field is a autoincrement field
  TIMESTAMP_FLAG = 1024;        // Field is a timestamp
  SET_FLAG = 2048;              // field is a set
  NO_DEFAULT_VALUE_FLAG = 4096; // Field doesn't have default value
  ON_UPDATE_NOW_FLAG = 8192;    // Field is set to NOW on UPDATE
  NUM_FLAG = 32768;             // Field is num (for clients)
  PART_KEY_FLAG = 16384;        // Intern; Part of some key
  GROUP_FLAG = 32768;           // Intern: Group field
  UNIQUE_FLAG = 65536;          // Intern: Used by sql_yacc
  BINCMP_FLAG = 131072;         // Intern: Used by sql_yacc

  // Server and Client Capabilities
  CLIENT_CLIENT_MYSQL = 1;             // new more secure passwords
  CLIENT_FOUND_ROWS = 2;                // Found instead of affected rows
  CLIENT_LONG_FLAG = 4;               // Get all column flags
  CLIENT_CONNECT_WITH_DB = 8;           // One can specify db on connect
  CLIENT_NO_SCHEMA = 16;              // Don't allow database.table.column
  CLIENT_COMPRESS = 32;               // Can use compression protocol
  CLIENT_ODBC = 64;                   // Odbc client
  CLIENT_LOCAL_FILES = 128;             // Can use LOAD DATA LOCAL
  CLIENT_IGNORE_SPACE = 256;            // Ignore spaces before '('
  CLIENT_PROTOCOL_41 = 1 shl 9;         // New 4.1 protocol
  CLIENT_INTERACTIVE = 1 shl 10;        // This is an interactive client
  CLIENT_SSL = 1 shl 11;                // Switch to SSL after handshake
  CLIENT_IGNORE_SIGPIPE = 1 shl 12;       // IGNORE sigpipes
  CLIENT_TRANSACTIONS = 1 shl 13;         // Client knows about transactions
  CLIENT_RESERVED = 1 shl 14;            // Old flag for 4.1 protocol
  CLIENT_SECURE_CONNECTION = 1 shl 15;  // Old flag for 4.1 authentication
  CLIENT_MULTI_STATEMENTS = 1 shl 16;   // Enable/disable multi-stmt support
  CLIENT_MULTI_RESULTS = 1 shl 17;      // Enable/disable multi-results
  CLIENT_PS_MULTI_RESULTS = 1 shl 18;   // Multi-results in PS-protocol
  CLIENT_PLUGIN_AUTH = 1 shl 19;        // Client supports plugin authentication
  CLIENT_CONNECT_ATTRS = 1 shl 20;      // Client supports connection attributes
  CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 1 shl 21;  // Enable authentication response packet to be larger than 255 bytes.
  CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS = 1 shl 22;    // Don't close the connection for a connection with expired password.
  CLIENT_SESSION_TRACK = 1 shl 23;      // Capable of handling server state change information. Its a hint to the server to include the state change information in Ok packet.
  CLIENT_DEPRECATE_EOF = 1 shl 24;      // Client no longer needs EOF packet
  // CLIENT_OPTIONAL_RESULTSET_METADATA = 1 shl 25;  // client can handle optional metadata information in the resultset
  CLIENT_ZSTD_COMPRESSION_ALGORITHM = 1 shl 26; // Client sets this flag when it is configured to use zstd compression method
  //CLIENT_QUERY_ATTRIBUTES = 1 shl 27; // Can send the optional part containing the query parameter set(s)
  CLIENT_CAPABILITY_EXTENSION = 1 shl 29; // reserved for futur use. (Was CLIENT_PROGRESS Client support progress indicator before 10.2)
  CLIENT_SSL_VERIFY_SERVER_CERT = 1 shl 30;
  CLIENT_REMEMBER_OPTIONS = 1 shl 31;

  MARIADB_CLIENT_PROGRESS = 1 shl 32;              // Client support progress indicator (since 10.2)
  MARIADB_CLIENT_COM_MULTI = 1 shl 33;             // Permit COM_MULTI protocol
  MARIADB_CLIENT_STMT_BULK_OPERATIONS = 1 shl 34;  // Permit bulk insert
  MARIADB_CLIENT_EXTENDED_TYPE_INFO = 1 shl 35;    // add extended metadata information
  MARIADB_CLIENT_CACHE_METADATA = 1 shl 36;        // permit skipping metadata

type
  enum_MYSQL_types = (
    MYSQL_TYPE_DECIMAL,
    MYSQL_TYPE_TINY,
    MYSQL_TYPE_SHORT,
    MYSQL_TYPE_LONG,
    MYSQL_TYPE_FLOAT,
    MYSQL_TYPE_DOUBLE,
    MYSQL_TYPE_NULL,
    MYSQL_TYPE_TIMESTAMP,
    MYSQL_TYPE_LONGLONG,
    MYSQL_TYPE_INT24,
    MYSQL_TYPE_DATE,
    MYSQL_TYPE_TIME,
    MYSQL_TYPE_DATETIME,
    MYSQL_TYPE_YEAR,
    MYSQL_TYPE_NEWDATE,
    MYSQL_TYPE_VARCHAR,
    MYSQL_TYPE_BIT,
    MYSQL_TYPE_TIMESTAMP2,
    MYSQL_TYPE_DATETIME2,
    MYSQL_TYPE_TIME2,
    MYSQL_TYPE_TYPED_ARRAY, // Used for replication only
    MYSQL_TYPE_INVALID := 243,
    MYSQL_TYPE_BOOL := 244, // Currently just a placeholder
    MYSQL_TYPE_JSON := 245,
    MYSQL_TYPE_NEWDECIMAL := 246,
    MYSQL_TYPE_ENUM := 247,
    MYSQL_TYPE_SET := 248,
    MYSQL_TYPE_TINY_BLOB := 249,
    MYSQL_TYPE_MEDIUM_BLOB := 250,
    MYSQL_TYPE_LONG_BLOB := 251,
    MYSQL_TYPE_BLOB := 252,
    MYSQL_TYPE_VAR_STRING := 253,
    MYSQL_TYPE_STRING := 254,
    MYSQL_TYPE_GEOMETRY := 255
    );

function MySQLDataType(MySqlFieldType: enum_MYSQL_types; decimals: integer; size: uint32; flags, charsetnr: integer;
  var ADataType: TFieldType; var ADecimals, ASize: integer): boolean;
begin
  Result := True;
  ASize := 0;
  case MySqlFieldType of
    MYSQL_TYPE_LONGLONG: ADatatype := ftLargeint;
    MYSQL_TYPE_TINY, MYSQL_TYPE_SHORT, MYSQL_TYPE_YEAR:
      if flags and UNSIGNED_FLAG <> 0 then
        ADatatype := ftWord
      else
        ADatatype := ftSmallint;
    MYSQL_TYPE_LONG, MYSQL_TYPE_INT24:
      if flags and AUTO_INCREMENT_FLAG <> 0 then
        ADatatype := ftAutoInc
      else
        ADatatype := ftInteger;
    MYSQL_TYPE_NEWDECIMAL, MYSQL_TYPE_DECIMAL:
    begin
      ADecimals := decimals;
      if (ADecimals < 5) and (Size - 2 - ADecimals < 15) then //ASize is display size i.e. with sign and decimal point
        ADatatype := ftBCD
      else if (ADecimals = 0) and (Size < 20) then
        ADatatype := ftLargeInt
      else
        ADatatype := ftFmtBCD;
      ASize := ADecimals;
    end;
    MYSQL_TYPE_FLOAT, MYSQL_TYPE_DOUBLE: ADatatype := ftFloat;
    MYSQL_TYPE_TIMESTAMP, MYSQL_TYPE_DATETIME: ADatatype := ftDateTime;
    MYSQL_TYPE_DATE: ADatatype := ftDate;
    MYSQL_TYPE_TIME: ADatatype := ftTime;
    MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_STRING, MYSQL_TYPE_ENUM, MYSQL_TYPE_SET:
    begin
      if MySqlFieldType = MYSQL_TYPE_STRING then ADatatype :=
          ftFixedChar
      else
        ADatatype := ftString;
      if charsetnr = 63 then
      begin //BINARY vs. CHAR, VARBINARY vs. VARCHAR
        if ADatatype = ftFixedChar then
          ADatatype := ftBytes
        else
          ADatatype := ftVarBytes;
        ASize := Size;
      end
      else
        ASize := Size div 1 { ?? FConnectionCharsetInfo.mbmaxlen }; // we need the same space in our dataset
    end;
    MYSQL_TYPE_TINY_BLOB..MYSQL_TYPE_BLOB:
      if charsetnr = 63 then ADatatype := ftBlob
      else
        ADatatype := ftMemo;
    MYSQL_TYPE_BIT: ADatatype := ftLargeInt;
    else
      Result := False;
  end;
end;

{$WARN 5028 off : Local $1 "$2" is not used}
{$WARN 4046 off : Constructing a class "$1" with abstract method "$2"}
{$WARN 5062 off : Found abstract method: $1}

constructor TMariaDBConnector.Create;
begin
  FDataset := TBufDataset.Create(nil);
  Sock := TTCPBlockSocket.Create;
  Sock.ConnectionTimeout := 2000;
end;

destructor TMariaDBConnector.Destroy;
begin
  inherited;
  FDataset.Free;
  Sock.CloseSocket;
  Sock.Free;
end;

procedure TMariaDBConnector.DebugStr(Log: string);
begin
  if MariaDBDebug then writeln(Log);
end;

// ---------------------------
// 0 - PACKET
// https://mariadb.com/kb/en/0-packet/
// ---------------------------

procedure TMariaDBConnector.SendPacket(Buffer: rawbytestring);
var
  LenPackNum: rawbytestring; // always 4 bytes;
  Len: integer;
begin
  // make sure FPackNumber is correct
  Len := Length(Buffer);
  LenPackNum := Chr(Len and $FF) + Chr((Len and $FF00) shr 8) + Chr((Len and $FF0000) shr 16);
  LenPackNum := LenPackNum + Chr(FPackNumber);
  Buffer := LenPackNum + Buffer;
  Sock.SendString(Buffer);
  Inc(FPackNumber);
end;

function TMariaDBConnector.ReceivePacket(Timeout: integer): rawbytestring;
var
  LenPackNum: array[0..3] of byte; // always 4 bytes;
  Rc: integer;
  Len: integer;
  Num: integer;
  Buffer: rawbytestring;
begin
  // https://mariadb.com/kb/en/0-packet/
  // https://mariadb.com/kb/en/com_query/
  // https://mariadb.com/kb/en/clientserver-protocol/
  Rc := Sock.RecvBufferEx(@LenPackNum, 4, Timeout);
  if Rc <> 4 then
  begin
    FLastError := Sock.LastError;
    FLastErrorDesc := Sock.LastErrorDesc;
    exit('');
  end;
  Num := LenPackNum[3];
  Len := LenPackNum[0] + LenPackNum[1] shl 8 + LenPackNum[2] shl 16;

  if (Num <> FPackNumber) or (Len = 0) then
  begin
    FLastError := -1;
    FLastErrorDesc := 'Got packets out of order of wrong size';
    exit('');
  end;

  Inc(FPackNumber); // we only want a correct packet numer next time

  Buffer := '';
  SetLength(Buffer, Len);
  if Sock.RecvBufferEx(@Buffer[1], Len, Timeout) <> Len then
  begin
    FLastError := -1;
    FLastErrorDesc := 'Did not receive complete packet';
    exit('');
  end;

  Result := Buffer;

end;

// ---------------------------
// 4 - SERVER RESPONSE PACKETS
// https://mariadb.com/kb/en/4-server-response-packets/
// ---------------------------

function TMariaDBConnector._set_int(Value: uint64; Len: Integer): rawbytestring;
var
  j: byte;
begin
  Result := '';
  j := 0;
  while Len > 0 do
  begin
    Result := Result + Chr((Value and ($FF shl j)) shr j);
    Inc(j, 8);
    Dec(Len);
  end;
end;



function TMariaDBConnector._get_int(Buffer: rawbytestring; var Ps: uint32; Len: integer = -1): integer; // -1 = lenenc
var
  Int, j: uint32;
begin
  if len = -1 then // int<lenenc> Length-encoded integers
  begin
    // < 0xFB - Integer value is this 1 byte integer
    // 0xFB - NULL value
    // 0xFC - Integer value is encoded in the next 2 bytes (3 bytes total)
    // 0xFD - Integer value is encoded in the next 3 bytes (4 bytes total)
    // 0xFE - Integer value is encoded in the next 8 bytes (9 bytes total)
    Int := Ord(Buffer[Ps]);
    Inc(Ps);
    len := 0;
    if Int = $FB then exit(0); // NULL // we can't do that yet
    if Int = $FC then len := 2; // 2 bytes len
    if Int = $FD then len := 3; // 3 bytes len
    if Int = $FE then len := 8; // 8 bytes len
  end;
  if len > 0 then // int<fix> Fixed-length integers
  begin
    j := 0;
    Int := 0;
    while len > 0 do // this can probably be done better
    begin
      Int := Int + Ord(Buffer[Ps]) shl j;
      j := j + 8;
      Dec(len);
      Inc(Ps);
    end;
  end;
  Result := Int;
end;

function TMariaDBConnector._get_str(Buffer: rawbytestring; var Ps: uint32; Len: integer = -1): rawbytestring;
begin
  if len > 0 then // string<fix> Fixed-length strings
  begin
    Result := Copy(Buffer, Ps, len);
    Inc(Ps, len);
    exit;
  end;
  if len = -1 then // string<lenenc> Length-encoded strings
  begin
    len := _get_int(Buffer, Ps, len);
    Result := Copy(Buffer, Ps, len);
    Inc(Ps, Len);
    exit;
  end;
  if len = 0 then // string<NUL> Null-terminated strings  // string<EOF> End-of-file length string
  begin
    while (Buffer[Ps + len] <> #$00) and (Ps + len < Length(Buffer)) do Inc(len);
    Result := Copy(Buffer, Ps, len);
    Inc(Ps, len + 1); // incl NULL
    exit;
  end;
end;

function TMariaDBConnector._is_error(Buffer: rawbytestring): boolean;
var
  Ps: uint32;
  i: integer;
begin
  // https://mariadb.com/kb/en/err_packet/
  // ERROR PACKAGE      4B 00 00 {len} 02 {number} FF {status} 15 04 2332383030304163636573732064656E69656420...
  // int<1> ERR_Packet header = 0xFF
  // int<2> error code. see error list
  // if (errorcode == 0xFFFF) /* progress reporting */
  // - int<1> stage
  // - int<1> max_stage
  // - int<3> progress
  // - string<lenenc> progress_info
  // else
  // - if (next byte = '#')
  // - - string<1> sql state marker '#'
  // - - string<5>sql state
  // - - string<EOF> human-readable error message
  // - else
  // - - string<EOF> human-readable error message
  Result := False; // no error
  if (length(Buffer) > 4) and (Buffer[1] = #$FF) then
  begin
    DebugStr('Answer from server: ' + Buf2Hex(buffer));
    DebugStr('We have an error ' + Ord(Buffer[1]).ToString);
    Ps := 2;
    i := _get_int(Buffer, Ps, 2);
    DebugStr('Error code: ' + i.ToString + ',  see https://mariadb.com/kb/en/mariadb-error-codes/');
    FLastError := i;
    FLastErrorDesc := '';
    if (i <> $ffff) then
    begin
      i := 5;
      if (buffer[4] = '#') then Inc(i, 5);
      FLastErrorDesc := copy(buffer, i);
      DebugStr('Error mess: ' + FLastErrorDesc);
    end;
    exit(True); // error
  end;

end;

function TMariaDBConnector._is_ok(Buffer: rawbytestring): boolean;
var
  Ps: uint32;
begin
  // https://mariadb.com/kb/en/ok_packet/
  // OK PACKAGE     07 00 00 {len} 02 {number} 00 {status} 00 00 02 00 00 00
  // int<1> 0x00 : OK_Packet header or (0xFE if CLIENT_DEPRECATE_EOF is set)
  // int<lenenc> affected rows
  // int<lenenc> last insert id
  // int<2> server status
  // int<2> warning count
  // if packet has more data
  // - string<lenenc> info
  // - if (status flags & SERVER_SESSION_STATE_CHANGED) and session_tracking_supported (see CLIENT_SESSION_TRACK)
  // - - string<lenenc> session state info
  Result := False;
  if (length(Buffer) > 4) and (Ord(Buffer[1]) = 0) then
  begin
    Ps := 2;
    FRowsAffected := _get_int(Buffer, Ps);
    FLastInsertId := _get_int(Buffer, Ps);
    FServerStatus := _get_int(Buffer, Ps, 2);
    FWarningCount := _get_int(Buffer, Ps, 2);

    DebugStr('Answer from server: ' + Buf2Hex(buffer));
    DebugStr('We have success ' + Ord(Buffer[1]).ToString);
    exit(True); // or do something else here
  end;
end;

function TMariaDBConnector._is_eof(Buffer: rawbytestring): boolean;
begin
  // https://mariadb.com/kb/en/eof_packet/
  // FE 00 00 22 00
  // int<1> 0xfe : EOF header
  // int<2> warning count
  // int<2> server status
  Result := (length(Buffer) = 5) and (Buffer[1] = #$FE);
end;

// ---------------------------
// 1 - CONNECTING
// https://mariadb.com/kb/en/1-connecting/
// https://mariadb.com/kb/en/connection/
// ---------------------------

function TMariaDBConnector.ConnectAndLogin(AServer, APort, AUser, APassword, ADatabase: rawbytestring): boolean;
var
  AuthPlugin: string;
  Buffer: rawbytestring = '';
  Seed: rawbytestring;

  FServerProtocol: integer;
  FServerVersion: rawbytestring;
  FConnectionId: integer;
  FServerCapabilities: uint64;
  FServerDefaultCollation: integer;
  FStatusFlags: integer;
  FPluginDataLength: integer;

  FClientCapabilities: uint64;

    Part1, Part2: rawbytestring;
  Ps: uint32;
  x: uint64;
begin
  Result := False;
  if AServer = '' then AServer := '127.0.0.1';
  if APort = '' then APort := '3306';

  Sock.Connect(AServer, APort);
  if Sock.LastError <> 0 then
  begin
    FLastError := Sock.LastError;
    FLastErrorDesc := Sock.LastErrorDesc;
    exit(False);
  end;

  // https://mariadb.com/kb/en/connection/#initial-handshake-packet

  FPackNumber := 0; // command always start on 0

  // read complete handschake package from the server
  Buffer := ReceivePacket(2000);
  if (FLastError <> 0) then exit(False);

  DebugStr('We have contact');
  DebugStr(Buf2Hex(Buffer));

  Ps := 1;

  FServerProtocol := _get_int(Buffer, Ps, 1); // int<1> protocol version
  FServerVersion := _get_str(Buffer, Ps, 0); // string<NUL> server version (MariaDB server version is by default prefixed by "5.5.5-")
  FConnectionId := _get_int(Buffer, Ps, 4); // int<4> connection id, not read here
  seed := _get_str(Buffer, Ps, 8); // string<8> scramble 1st part (authentication seed)
  Inc(Ps, 1); // string<1> reserved byte

  FServerCapabilities := _get_int(Buffer, Ps, 2); // int<2> server capabilities (1st part)
  FServerDefaultCollation := _get_int(Buffer, Ps, 1); // int<1> server default collation
  FStatusFlags := _get_int(Buffer, Ps, 2); // int<2> status flags
  x := _get_int(Buffer, Ps, 2);
  x := x shl 16;
  FServerCapabilities := FServerCapabilities + x; // int<2> server capabilities (2nd part)
  if (FServerCapabilities and CLIENT_PLUGIN_AUTH) <> 0 then
    FPluginDataLength := _get_int(Buffer, Ps, 1) // - int<1> plugin data length or - int<1> 0x00
  else
    FPluginDataLength := 0;
  Inc(Ps, 6); // string<6> filler
  if (FServerCapabilities and CLIENT_CLIENT_MYSQL) = 0 then
  begin
    x := _get_int(Buffer, Ps, 4);
    x := x shl 32;
    FServerCapabilities := FServerCapabilities + x; // - int<4> server capabilities 3rd part . MariaDB specific flags /* MariaDB 10.2 or later */
  end
  else
    Inc(Ps, 4); // string<4> filler
  if (FServerCapabilities and CLIENT_SECURE_CONNECTION) <> 0 then
  begin
    //DebugStr('getting 2nd seed');
    Seed := Seed + _get_str(Buffer, Ps, Math.Max(12, FPluginDataLength - 9)); // string<n> scramble 2nd part. Length = max(12, plugin data length - 9)
    Inc(Ps, 1); // string<1> reserved byte
  end;
  if (FServerCapabilities and CLIENT_PLUGIN_AUTH) <> 0 then
  begin
    //DebugStr('getting authplugin');
    AuthPlugin := _get_str(Buffer, Ps, 0); // string<NUL> authentication plugin name
  end;

  DebugStr('------------------');
  DebugStr('Server protocol: ' + FServerProtocol.ToString);
  DebugStr('Sserver version: ' + FServerVersion);
  DebugStr('Connection ID: ' + FConnectionId.ToString);
  DebugStr('Server capabilities: ' + BinStr(FServerCapabilities, 64) );  // 00000000000000001111011111111110
  DebugStr('Server default collation: ' + FServerDefaultCollation.ToString);
  DebugStr('Status flag: ' + FStatusFlags.ToString);
  DebugStr('Plugin data length: ' + FPluginDataLength.ToString);
  DebugStr('Authentication plugin: ' + AuthPlugin);
  DebugStr('------------------');

  // SHA1( password )    XOR    SHA1( seed + SHA1( SHA1( password ) ) )
  Part1 := SHA1(APassword);
  Part2 := SHA1(seed + SHA1(SHA1(APassword)));
  for Ps := 1 to length(Part1) do
    Part1[Ps] := Chr(Ord(Part1[PS]) xor Ord(Part2[Ps]));

  DebugStr('Password seed: ' + Buf2Hex(seed));
  DebugStr('Hashed pw: ' + Buf2Hex(Part1));
  DebugStr('------------------');

  if AuthPlugin <> 'mysql_native_password' then
  begin
    FLastError := -1;
    FLastErrorDesc := 'Authentication "' + AuthPlugin + '" not implemented yet';
    DebugStr(FLastErrorDesc);
  end;

  if AuthPlugin = 'mysql_native_password' then
  begin

    FClientCapabilities := CLIENT_CLIENT_MYSQL or
      CLIENT_LONG_FLAG or CLIENT_CONNECT_WITH_DB or CLIENT_PROTOCOL_41 or CLIENT_INTERACTIVE or
      CLIENT_TRANSACTIONS or CLIENT_SECURE_CONNECTION or CLIENT_MULTI_STATEMENTS or CLIENT_MULTI_RESULTS;

    // Construct the answer handschake package
    Buffer := _set_int(FClientCapabilities, 4);  // int<4> client capabilities // #$0D#$A6#$03#$00
    Buffer := Buffer + _set_int($01000000, 4); // int<4> max packet size // #0#0#0#1
    Buffer := Buffer + #$21;           // int<1> client character collation
    Buffer := Buffer + #0#0#0#0#0#0#0#0#0#0#0#0#0#0#0#0#0#0#0; // string<19> reserved
    Buffer := Buffer + #0#0#0#0;       // - int<4> extended client capabilities
    Buffer := Buffer + AUser + #0;     // string<NUL> username
    Buffer := Buffer + #$14 + Part1;   // - string<fix> authentication response (length is indicated by previous field)
    Buffer := Buffer + ADatabase + #0; // - string<NUL> default database name

    DebugStr(Buf2Hex(Buffer));
    DebugStr('sending first packet with authentication');
    SendPacket(Buffer);

    // we need an answer
    Buffer := ReceivePacket(5000);
    if (FLastError <> 0) then exit(False);

    if _is_error(Buffer) then exit(False);
    if _is_ok(Buffer) then exit(True);

  end;
end;

// ---------------------------
// 2 - TEXT Protocol
// https://mariadb.com/kb/en/2-text-protocol/
// https://mariadb.com/kb/en/result-set-packets/
// ---------------------------

function TMariaDBConnector.ExecuteCommand(Command: MySqlCommands; SQL: rawbytestring = ''): boolean;
var
  Buffer: rawbytestring;
  Value: rawbytestring;
  Column: integer;
  Ps, MaxLen: uint32;

  sqltype: enum_MYSQL_types;
  decimals, flags, charsetnr: integer;

  size: uint32;
  AName: string;
  ADataType: TFieldType;
  ADecimals: integer;
  ASize: integer;
  dt: tdatetime;
begin

  Result := True;
  Dataset.Clear;
  FLastError := 0;
  FLastErrorDesc := '';
  FPackNumber := 0; // command always start on 0

  Buffer := Chr(Ord(Command)) + SQL;
  DebugStr('Sending ' + GetEnumName(TypeInfo(MySqlCommands), Ord(Command)) + ' ' + SQL);
  SendPacket(Buffer);

  Buffer := ReceivePacket(2000);
  // if MariaDBDebug then DebugStr(buf2hex(Buffer));
  if (FLastError <> 0) then exit(False);
  if _is_error(Buffer) then exit(False);
  if _is_eof(Buffer) then exit(True); // for COMMAND_SET_OPTION FE 00 00 02 00
  if Buffer = '' then exit(True); // no resultset

  Column := Ord(Buffer[1]);
  DebugStr('Columns # ' + Column.ToString);
  if Column = 0 then exit(True); // no resultset

  // get columns
  repeat

    Buffer := ReceivePacket(2000);
    if _is_error(Buffer) then break;
    if _is_ok(Buffer) then break;
    if _is_eof(Buffer) then break;

    Ps := 1; // string<lenenc> catalog (always 'def')
    Value := _get_str(Buffer, Ps); // string<lenenc> catalog (always 'def')
    if Value <> 'def' then;
    Value := _get_str(Buffer, Ps); // string<lenenc> schema
    Value := _get_str(Buffer, Ps); // string<lenenc> table alias
    Value := _get_str(Buffer, Ps); // string<lenenc> table
    AName := _get_str(Buffer, Ps); // string<lenenc> column alias
    Value := _get_str(Buffer, Ps); // string<lenenc> column

    ADataType := ftString; // default if error
    ASize := 1024;
    ADecimals := 0;
    if Buffer[Ps] = #$0C then // int<lenenc> length of fixed fields (=0xC)
    begin
      Inc(Ps);
      charsetnr := _get_int(Buffer, Ps, 2); // int<2> character set number
      size := _get_int(Buffer, Ps, 4);      // int<4> max. column size
      sqltype := enum_MYSQL_types(_get_int(Buffer, Ps, 1)); // int<1> Field types
      flags := _get_int(Buffer, Ps, 2);     // int<2> Field detail flag
      decimals := _get_int(Buffer, Ps, 1);  // int<1> decimals
      if not MySQLDataType(sqltype, decimals, size, flags, charsetnr, ADataType, ADecimals, ASize) then
      begin
        ADataType := ftString; // default if error
        ASize := 1024;
        ADecimals := 0;
      end;
    end;

    DebugStr('Adding column ' + AName + ' ' + GetEnumName(TypeInfo(TFieldType), Ord(ADataType)) + ' size:' + ASize.ToString);
    FDataset.FieldDefs.Add(AName, ADataType, ASize);

  until (Buffer = ''); // never happen, we get eof, ok or error first

  if FDataset.FieldDefs.Count = 0 then exit(False); // shouldn't happen

  FDataset.CreateDataset;
  FDataset.Open;
  // copy all data to a TBufDataset

  repeat
    Buffer := ReceivePacket(2000);
    if _is_error(Buffer) then break;
    if _is_ok(Buffer) then break;
    if _is_eof(Buffer) then break;

    FDataset.Insert;
    Column := -1;
    MaxLen := Length(Buffer);
    Ps := 1;

    // DebugStr(Buf2Hex(Buffer));
    while Ps < MaxLen do
    begin
      Inc(Column);
      FDataset.Fields[Column].Clear;

      if Buffer[Ps] = #$FB then
      begin
        Inc(Ps);
        continue; // NULL
      end;

      // we are in TEXT protocol so result is always in text
      Value := _get_str(Buffer, Ps);

      case FDataset.Fields[Column].DataType of

        ftFixedChar, ftString: FDataset.Fields[Column].AsString := Value; // no dots, so just assign
        ftBytes, ftVarBytes, ftBlob, ftMemo: FDataset.Fields[Column].AsString := Value; // no dots, so just assign
        ftInteger, ftLargeint, ftWord, ftSmallint, ftAutoInc: FDataset.Fields[Column].AsString := Value; // no dots, so just assign

        ftBCD, ftFmtBCD, ftFloat:
        begin
          Value := StringReplace(Value, '.', DefaultFormatSettings.DecimalSeparator, []);
          FDataset.Fields[Column].AsString := Value;
        end;

        ftDateTime, ftDate, ftTime:
        begin
          if FDataset.Fields[Column].DataType = ftDateTime then dt := ScanDateTime('yyyy-mm-dd hh:nn:ss', Value);
          if FDataset.Fields[Column].DataType = ftDate then dt := ScanDateTime('yyyy-mm-dd', Value);
          if FDataset.Fields[Column].DataType = ftTime then dt := ScanDateTime('hh:nn:ss', Value);
          FDataset.Fields[Column].AsDateTime := dt;
        end;

        else
          FDataset.Fields[Column].AsString := Value; // no dots, so just assign

      end;
    end;

    FDataset.Post;

  until (Buffer = ''); // never happen, we get eof, ok or error first

  DebugStr('Data copied to Dataset');

end;


function TMariaDBConnector.Query(SQL: string): boolean;
begin
  Result := ExecuteCommand(COMMAND_QUERY, SQL);
end;

function TMariaDBConnector.Ping: boolean;
begin
  Result := ExecuteCommand(COMMAND_PING);
end;

procedure TMariaDBConnector.Quit;
begin
  ExecuteCommand(COMMAND_QUIT);
end;

procedure TMariaDBConnector.SetMultiOptions(Value: boolean);
var
  Val: rawbytestring;
begin
  Val := #0#0;
  if Value then Val := #1#0;
  ExecuteCommand(COMMAND_SET_OPTION, Val);
end;

// ---------------------------
// 3 - BINARY Protocol
// https://mariadb.com/kb/en/3-binary-protocol-prepared-statements/
// ---------------------------

begin


end.
