unit umariadbconnector;

{$mode ObjFPC}{$H+}
{.$WARN 5091 off : Local variable "$1" of a managed type does not seem to be initialized}

interface

uses
  SysUtils, blcksock, synacode,
  strutils, DB, BufDataset,
  typinfo, DateUtils;

const
  MariaDbDebug: boolean = False;

type
  MySqlCommands = (COMMAND_SLEEP, COMMAND_QUIT, COMMAND_INIT_DB, COMMAND_QUERY,
    COMMAND_MYSQL_LIST, COMMAND_CREATE_DB, COMMAND_DROP_DB, COMMAND_REFRESH,
    COMMAND_SHUTDOWN, COMMAND_STATISTICS, COMMAND_PROCESS_INFO, COMMAND_CONNECT,
    COMMAND_PROCESS_KILL, COMMAND_DEBUG, COMMAND_PING, COMMAND_TIME,
    COMMAND_DELAYED_INSERT, COMMAND_CHANGE_USER, COMMAND_BINLOG_DUMP,
    COMMAND_TABLE_DUMP, COMMAND_CONNECT_OUT);

type
  TMariaDBConnector = class(TObject)
    Sock: TTCPBlockSocket;
    FLastError: integer;
    FLastErrorDesc: string;
    FPackNumber: byte;
    FDataset: TBufDataset;
  private
    procedure DebugStr(Log: string);
    function _is_error(Buffer: rawbytestring): boolean;
    function _is_ok(Buffer: rawbytestring): boolean;
    function _is_eof(Buffer: rawbytestring): boolean;
  public
    constructor Create;
    destructor Destroy; override;
    procedure SendPacket(Buffer: rawbytestring);
    function ReceivePacket(Timeout: integer): rawbytestring;
    function ConnectAndLogin(AServer, APort, AUser, APassword, ADatabase: rawbytestring): boolean;
    function ExecuteCommand(Command: MySqlCommands; SQL: string = ''): boolean;
    function Query(SQL: string): boolean;
    function Ping: boolean;
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
  NOT_NULL_FLAG = 1;       //  Field can't be NULL
  PRIMARY_KEY_FLAG = 2;        //  Field is part of a primary key
  UNIQUE_KEY_FLAG = 4;     //  Field is part of a unique key
  MULTIPLE_KEY_FLAG = 8;   //  Field is part of a key
  BLOB_FLAG = 16;          //  Field is a blob
  UNSIGNED_FLAG = 32;      //  Field is unsigned
  ZEROFILL_FLAG = 64;      //  Field is zerofill
  BINARY_FLAG = 128;       //  Field is binary
  ENUM_FLAG = 256;            // field is an enum
  AUTO_INCREMENT_FLAG = 512;  // field is a autoincrement field
  TIMESTAMP_FLAG = 1024;      // Field is a timestamp
  SET_FLAG = 2048;            // field is a set
  NO_DEFAULT_VALUE_FLAG = 4096; // Field doesn't have default value
  ON_UPDATE_NOW_FLAG = 8192;    // Field is set to NOW on UPDATE
  NUM_FLAG = 32768;           // Field is num (for clients)
  PART_KEY_FLAG = 16384;      // Intern; Part of some key
  GROUP_FLAG = 32768;         // Intern: Group field
  UNIQUE_FLAG = 65536;        // Intern: Used by sql_yacc
  BINCMP_FLAG = 131072;       // Intern: Used by sql_yacc

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
        ASize := Size div 3 { ?? FConnectionCharsetInfo.mbmaxlen };
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

function GetString(var Buffer: rawbytestring; var i: uint32): rawbytestring;
var
  Len, x, j: uint32;
begin
  Result := '';
  // < 0xFB - Integer value is this 1 byte integer
  // 0xFB - NULL value
  // 0xFC - Integer value is encoded in the next 2 bytes (3 bytes total)
  // 0xFD - Integer value is encoded in the next 3 bytes (4 bytes total)
  // 0xFE - Integer value is encoded in the next 8 bytes (9 bytes total)
  Len := Ord(Buffer[i]);
  Inc(i);
  if Buffer[i] = #$FB then exit(''); // NULL
  x := 0;
  if Len = $FC then x := 2; // 2 bytes len
  if Len = $FD then x := 3; // 3 bytes len
  if Len = $FE then x := 8; // 8 bytes len
  if x > 0 then
  begin
    j := 0;
    Len := 0;
    while x > 0 do
    begin
      Len := Len + Ord(Buffer[i]) shl j;
      j := j + 8;
      Dec(x);
      Inc(i);
    end;
  end;
  // we are in TEXT protocol so result is always in text
  Result := Copy(Buffer, i, Len);
  Inc(i, Len); // string<lenenc> column
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

function TMariaDBConnector._is_error(Buffer: rawbytestring): boolean;
var
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
    i := Ord(buffer[2]) + Ord(buffer[3]) shl 8;
    DebugStr('Error code: ' + i.ToString + ',  see https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html');
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
  ServerVersion: rawbytestring;
  Part1, Part2: rawbytestring;
  i: integer;
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

  i := 1; // pack len [j]
  DebugStr('protocol: ' + Ord(Buffer[i]).ToString); // int<1> protocol version
  if Ord(Buffer[i]) = 10 then
    DebugStr('client_capabilities = 1');
  Inc(i);

  ServerVersion := ''; // string<NUL> server version (MariaDB server version is by default prefixed by "5.5.5-")
  while Buffer[i] <> #0 do
  begin
    ServerVersion := ServerVersion + Buffer[i];
    Inc(i);
  end;
  DebugStr('server: ' + ServerVersion);
  Inc(i);

  // int<4> connection id, not read here
  Inc(i, 4);

  seed := Copy(Buffer, i, 8); // string<8> scramble 1st part (authentication seed)
  DebugStr('1st seed: ' + Buf2Hex(seed));
  Inc(i, 8);
  Inc(i, 1); // string<1> reserved byte

  if Length(Buffer) >= i + 1 then
    Inc(i, 1);

  // int<2> server capabilities (1st part)
  // int<1> server default collation
  // int<2> status flags
  // int<2> server capabilities (2nd part)
  // if (server_capabilities & PLUGIN_AUTH)
  // - int<1> plugin data length
  // else
  // - int<1> 0x00
  // string<6> filler
  // if (server_capabilities & CLIENT_MYSQL)
  // - string<4> filler
  // else
  // - int<4> server capabilities 3rd part . MariaDB specific flags /* MariaDB 10.2 or later */
  // if (server_capabilities & CLIENT_SECURE_CONNECTION)
  // - string<n> scramble 2nd part . Length = max(12, plugin data length - 9)
  // - string<1> reserved byte
  // if (server_capabilities & PLUGIN_AUTH)
  // - string<NUL> authentication plugin name

  (*
  if Length(Buffer) >= i + 18 then
  begin
    { Get server_language, not read here }
    { Get server_status, not read here }
  end;
  *)
  Inc(i, 18 - 1);
  if Length(Buffer) >= i + 12 - 1 then
    seed := seed + Copy(Buffer, i, 12);
  DebugStr('incl. 2nd part seed: ' + Buf2Hex(seed));

  Inc(i, 12 + 1);

  AuthPlugin := ''; // null terminated version
  while (Buffer[i] <> #0) do
  begin
    AuthPlugin := AuthPlugin + Buffer[i];
    Inc(i);
  end;
  DebugStr('Authentication Plugin: ' + AuthPlugin);

  // SHA1( password )    XOR    SHA1( seed + SHA1( SHA1( password ) ) )
  Part1 := SHA1(APassword);
  Part2 := SHA1(seed + SHA1(SHA1(APassword)));
  for i := 1 to length(Part1) do
    Part1[i] := Chr(Ord(Part1[i]) xor Ord(Part2[i]));
  DebugStr('hashed pw: ' + Buf2Hex(Part1));

  if AuthPlugin <> 'mysql_native_password' then
  begin
    FLastError := -1;
    FLastErrorDesc := 'Authentication "' + AuthPlugin + '" not implemented yet';
    DebugStr(FLastErrorDesc);
  end;

  if AuthPlugin = 'mysql_native_password' then
  begin

    // Construct the answer handschake package
    Buffer := #$0D#$A6#$03#$00;        // int<4> client capabilities
    Buffer := Buffer + #0#0#0#1;       // int<4> max packet size
    Buffer := Buffer + #$21;           // int<1> client character collation
    Buffer := Buffer + #0#0#0#0#0#0#0#0#0#0#0#0#0#0#0#0#0#0#0; // string<19> reserved
    Buffer := Buffer + #0#0#0#0;       // - int<4> extended client capabilities
    Buffer := Buffer + AUser + #0;     // string<NUL> username
    Buffer := Buffer + #$14 + Part1;   // - string<fix> authentication response (length is indicated by previous field)
    Buffer := Buffer + ADatabase + #0; // - string<NUL> default database name

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

function TMariaDBConnector.ExecuteCommand(Command: MySqlCommands; SQL: string = ''): boolean;
var
  Buffer: rawbytestring;
  Value: rawbytestring;
  Column: integer;
  Ps, MaxLen: uint32;
  MySqlFieldType: enum_MYSQL_types;
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
  if (FLastError <> 0) then exit(False);
  if _is_error(Buffer) then exit(False);
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
    Value := GetString(Buffer, Ps); // string<lenenc> catalog (always 'def')
    if Value <> 'def' then;
    Value := GetString(Buffer, Ps); // string<lenenc> schema
    Value := GetString(Buffer, Ps); // string<lenenc> table alias
    Value := GetString(Buffer, Ps); // string<lenenc> table
    AName := GetString(Buffer, Ps); // string<lenenc> column alias
    Value := GetString(Buffer, Ps); // string<lenenc> column

    ADataType := ftString; // default if error
    ASize := 1024;
    ADecimals := 0;
    if Buffer[Ps] = #$0C then // int<lenenc> length of fixed fields (=0xC)
    begin
      Inc(Ps); // int<2> character set number
      charsetnr := Ord(Buffer[Ps]) + Ord(Buffer[Ps + 1]) shl 8;
      Inc(Ps, 2); // int<4> max. column size
      size := Ord(Buffer[Ps]) + Ord(Buffer[Ps + 1]) shl 8 + Ord(Buffer[Ps + 2]) shl 16 + Ord(Buffer[Ps + 3]) shl 24; // int<4> max. column size
      Inc(Ps, 4); // int<1> Field types
      MySqlFieldType := enum_MYSQL_types(Buffer[Ps]); // int<1> Field types
      Inc(Ps); // int<2> Field detail flag
      flags := Ord(Buffer[Ps]) + Ord(Buffer[Ps + 1]) shl 8; // int<2> Field detail flag
      Inc(Ps, 2); // int<1> decimals
      decimals := Ord(Buffer[Ps]); // int<1> decimals
      if not MySQLDataType(MySqlFieldType, decimals, size, flags, charsetnr, ADataType, ADecimals, ASize) then
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

    // writeln(Buf2Hex(Buffer));
    while Ps < MaxLen do
    begin
      Inc(Column);
      FDataset.Fields[Column].Clear;

      if Buffer[Ps] = #$FB then continue; // NULL

      // we are in TEXT protocol so result is always in text
      Value := GetString(Buffer, Ps);

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

// ---------------------------
// 3 - BINARY Protocol
// https://mariadb.com/kb/en/3-binary-protocol-prepared-statements/
// ---------------------------

begin


end.
