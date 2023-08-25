program mytest;

{$MODE OBJFPC}{$H+}

uses
  Classes,
  SysUtils,
  StrUtils,
  Types,
  DB,
  JsonConf,
  {$IFDEF WINDOWS}
    Windows, {for setconsoleoutputcp}
  {$ENDIF}
  umariadbconnector;

const
  CRLF = #13#10;
  clrNormal = #27'[0m';
  clrBold = #27'[1m';
  clrRed = #27'[31m';
  clrGreen = #27'[32m';
  clrYellow = #27'[33m';
  clrWhite = #27'[37m';

var
  ConfigFileName: string;
  Server: string;
  User: string;
  Password: string;
  Database: string;
  MDB: TMariaDBConnector;

  // ---------------------------
  // get single value from database
  // ---------------------------
  function GetSQLValue(SQL: string; Column: integer = 1): string;
  begin
    Result := '';
    if MDB.Query(SQL) and MDB.Dataset.Active then
      if (MDB.Dataset.RecordCount > 0) and (Column <= MDB.Dataset.Fields.Count) then
        Result := MDB.Dataset.Fields[Column - 1].AsString;
  end;

  // ---------------------------
  // execute sql command and display result
  // ---------------------------
  function DoSQL(SQL: string): boolean;
  var
    fmt: string;
    i: integer;
    s, h1, h2: string;
    OneItem: boolean;
    Help: string;
  begin

    // if help command then always set quotes around topic
    Help := '';
    if (Pos('help ', sql) = 1) then Help := Copy(sql, 6);
    Help := AnsiDequotedStr(Help, #39);
    Help := AnsiDequotedStr(Help, '"');
    if Help <> '' then sql := 'help ' + QuotedStr(Help);

    // execute sql
    Result := MDB.Query(SQL);
    if not Result then writeln(clrRed + 'Error: ' + clrNormal, SQL, ' ', MDB.LastError, ' ', MDB.LastErrorDesc);

    if MDB.Dataset.Active then
    begin

      for i := 0 to MDB.Dataset.Fields.Count - 1 do
        if MDB.Dataset.Fields[i].Alignment = taLeftJustify then MDB.MaxColumnLength[i] := -MDB.MaxColumnLength[i];

      h1 := '+';
      for i := 0 to MDB.Dataset.Fields.Count - 1 do h1 := h1 + StringOfChar('-', Abs(MDB.MaxColumnLength[i]) + 2) + '+';

      h2 := '|';
      for i := 0 to MDB.Dataset.Fields.Count - 1 do
      begin
        h2 := h2 + format(' %' + MDB.MaxColumnLength[i].ToString + 's |', [MDB.Dataset.Fields[i].FieldName]);
      end;

      OneItem := not Assigned(MDB.Dataset.FindField('is_it_category'));

      if (Help <> '') and (MDB.Dataset.RecordCount = 0) then
      begin
        writeln('Nothing found');
        writeln('Please try to run `help contents` for a list of all accessible topics');
      end
      else if not OneItem then
      begin
        writeln('For more information, type `help <item>`, where <item> is one of the following');
      end;

      if Help = '' then
      begin
        writeln(h1);
        writeln(h2);
        writeln(h1);
      end;

      MDB.Dataset.First;
      while not MDB.Dataset.EOF do
      begin

        if Help = '' then
        begin
          h2 := '|';
          for i := 0 to MDB.Dataset.Fields.Count - 1 do
          begin
            fmt := ' %' + MDB.MaxColumnLength[i].ToString + 's |';
            s := MDB.Dataset.Fields[i].AsString;
            h2 := h2 + format(fmt, [s]);
          end;
          writeln(h2);

        end
        else
        begin

          if OneItem then
          begin

            h2 := '';
            for i := 0 to MDB.Dataset.Fields.Count - 1 do
              if MDB.Dataset.Fields[i].AsString <> '' then
              begin
                h2 := clrYellow + MDB.Dataset.Fields[i].FieldName + clrNormal + ': ';
                if Pos(#10, MDB.Dataset.Fields[i].AsString) > 0 then h2 := h2 + CRLF;
                s := MDB.Dataset.Fields[i].AsString;
                s := StringReplace(s, #10, CRLF, [rfReplaceAll]);
                h2 := h2 + s;
                writeln(h2);
              end;

          end
          else
          begin

            writeln(MDB.Dataset.FieldByName('name').AsString);

          end;

        end;

        MDB.Dataset.Next;
      end;

      if Help = '' then
      begin
        writeln(h1);
        writeln(format('%d rows in set', [MDB.Dataset.RecordCount]));
      end;

    end;

  end;

  // ---------------------------
  // create random string
  // ---------------------------
  function rand(len: SizeInt = 20): string;
  const
    chars: array of string = ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L',
      'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
      'Y', 'Z', 'Ä', 'Ö', 'Ü');
  var
    i, l: SizeInt;
    c: string;
  begin
    Result := ''; // no warning
    SetLength(Result, len);
    l := 0;
    for i := 1 to len - Random(10) do
    begin
      c := chars[Random(Length(chars))];
      if l + Length(c) > Length(Result) then
        SetLength(Result, Length(Result) * 2);
      Move(c[1], Result[l + 1], Length(c));
      Inc(l, Length(c));
    end;
    SetLength(Result, l);
  end;

  // ---------------------------
  // print help message
  // ---------------------------
  procedure printhelp;
  begin
    writeln('');
    writeln('List of all client commands:');
    writeln('');
    writeln('?         (\?)  Synonym for help.');
    writeln('help      (\h)  Display this help.');
    writeln('connect   (\c)  Disconnect and connect to new server.');
    writeln('exit      (\q)  Exit. Same as quit.');
    writeln('quit      (\q)  Quit.');
    writeln('info      (\i)  Get information about the server.');
    writeln('debug     (\d)  Toggle debug.');
    writeln('ping      (\p)  Send ping to server.');
    writeln('save      (\s)  Save credentials to file.');
    writeln('unsave    (\u)  Delete saved credentials.');
    writeln('test x    (\t1) drop and create test database with x rows.');
    writeln('untest    (\t2) cleanup test database.');
    writeln('');
    writeln('For server side help, type `help contents`.');
    writeln('or `help upper` or `help rep%`.');
    writeln('');
  end;

  // ---------------------------
  // load config with credentials
  // ---------------------------
  procedure loadconfig;
  var
    Conf: TJsonConfig;
  begin
    if FileExists(ConfigFileName) then
    begin
      Conf := TJSONConfig.Create(nil);
      try
        Conf.Formatted := True;
        Conf.FileName := ConfigFileName;
        Server := string(Conf.GetValue('Server', Server));
        User := string(Conf.GetValue('User', User));
        Password := string(Conf.GetValue('Password', Password));
        Database := string(Conf.GetValue('Database', Database));
      finally
        Conf.Free;
      end;
    end;
  end;

  // ---------------------------
  // save config with credentials
  // ---------------------------
  procedure saveconfig;
  var
    Conf: TJsonConfig;
  begin
    Conf := TJSONConfig.Create(nil);
    try
      Conf.Formatted := True;
      Conf.FileName := ConfigFileName;
      Conf.SetValue('Server', Server);
      Conf.SetValue('User', User);
      Conf.SetValue('Password', Password);
      Conf.SetValue('Database', Database);
      writeln('Credentials saved to file ' + ExtractFileName(ConfigFileName));
    finally
      Conf.Free;
    end;
  end;

  // ---------------------------
  // delete file with credentials
  // ---------------------------
  procedure unsaveconfig;
  begin
    SysUtils.DeleteFile(ConfigFileName);
    writeln('File with credentials deleted');
  end;

  // ---------------------------
  // dialog for entering credentials and connecting to server
  // ---------------------------
  procedure EnterOrCheckCredentialsAndConnect(Reconnect: boolean);
  var
    Value: string;
  begin
    if Reconnect then
    begin
      Write(format('Host [%s]: ', [Server]));
      readln(Value);
      if Value <> '' then Server := Value;
      Write(format('User [%s]: ', [User]));
      readln(Value);
      if Value <> '' then User := Value;
      Write(format('Password [***]: ', []));
      readln(Value);
      if Value <> '' then Password := Value;
    end;

    if (Server <> '') and (User <> '') and (Password <> '') then
    begin
      writeln(format('Connecting to %s as %s', [Server, User]));
      if not MDB.ConnectAndLogin(Server, '', User, Password, Database) then
        writeln('Problem connecting. ', MDB.LastError, ' ', MDB.LastErrorDesc);
    end;

    if MDB.Connected then
      Writeln('We have a connection. You can enter \s to save the credentials.')
    else
      Writeln('Not connected. Enter \c to connect.');

  end;

  // ---------------------------
  // create test database and table and add records
  // ---------------------------
  procedure createtestdatabase(sql: string);
  var
    cnt, i: integer;
    Arr: TStringDynArray;
  begin
    cnt := 600;
    Arr := SplitString(sql, ' ');
    if Length(Arr) > 1 then cnt := StrToIntDef(Arr[1], cnt);
    writeln('Create database connector_test');
    DoSQL('drop database if exists connector_test;');
    DoSQL('create database connector_test;');
    DoSQL('use connector_test;');
    writeln('Create table test');
    sql := 'create table if not exists test (';
    sql := sql + ' id bigint auto_increment primary key,';
    sql := sql + ' name varchar(20) charset utf8,';
    sql := sql + ' name2 varchar(30) charset utf8,';
    sql := sql + ' key name (name(5))';
    sql := sql + ') engine=InnoDB default charset latin1;';
    DoSQL(sql);
    writeln(Format('Inserting %d records', [cnt]));
    for i := 1 to cnt do
    begin
      if cnt mod 100 = 0 then Write(#13, round((i / cnt) * 100), '%');
      sql := rand;
      sql := format('INSERT INTO test (name, name2) VALUES (%s, %s)', [QuotedStr(sql), QuotedStr('test1')]);
      DoSQL(sql);
    end;
    writeln(#13, 'done');
  end;

  // ---------------------------
  // remove test database
  // ---------------------------
  procedure cleanuptest;
  begin
    writeln('Dropping database connector_test');
    DoSQL('DROP DATABASE IF EXISTS connector_test;');
  end;

  // ---------------------------
  // print status information from server
  // ---------------------------
  procedure printinfo;
  var
    info: string;
    Up: string;
  begin
    Info := '';

    Up := GetSQLValue('show global status like ''Uptime'';', 2);
    Up := FormatDateTime('d" days" h" hours" n" minutes" s" seconds"', StrToInt(Up) / (24 * 60 * 60) + 1);

    Info := Info + 'Server version          ' + GetSQLValue('select @@version') + CRLF;
    Info := Info + 'Protocol version        ' + GetSQLValue('select @@protocol_version') + CRLF;
    Info := Info + 'Uptime                  ' + Up + CRLF;
    Info := Info + 'Current database        ' + Database + CRLF;
    Info := Info + 'Current user            ' + User + CRLF;
    Info := Info + 'Compression             ' + GetSQLValue('show global status like ''Compression'';', 2) + CRLF;
    Info := Info + 'Server characterset     ' + GetSQLValue('select @@character_set_server') + CRLF;
    Info := Info + 'Db     characterset     ' + GetSQLValue('select @@character_set_database') + CRLF;
    Info := Info + 'Client characterset     ' + GetSQLValue('select @@character_set_client') + CRLF;
    Info := Info + 'Conn.  characterset     ' + GetSQLValue('select @@character_set_connection') + CRLF;
    Info := Info + 'autocommit              ' + GetSQLValue('select @@autocommit') + CRLF;
    Info := Info + 'have_compress           ' + GetSQLValue('select @@have_compress') + CRLF;
    Info := Info + 'have_openssl            ' + GetSQLValue('select @@have_openssl') + CRLF;
    Info := Info + 'have_ssl                ' + GetSQLValue('select @@have_ssl') + CRLF;
    Info := Info + 'hostname                ' + GetSQLValue('select @@hostname') + CRLF;
    Info := Info + 'net_buffer_length       ' + GetSQLValue('select @@net_buffer_length') + CRLF;
    Info := Info + 'max_allowed_packet      ' + GetSQLValue('select @@max_allowed_packet') + CRLF;

    writeln(Info);

  end;

  {$IFDEF WINDOWS}
  // ---------------------------
  // initialize windows console to print color
  // ---------------------------
  procedure InitWindows;
  const
    ENABLE_VIRTUAL_TERMINAL_PROCESSING = $0004;
  var
    dwOriginalOutMode, dwRequestedOutModes, dwRequestedInModes, dwOutMode: Dword;
  begin
    // SetTextCodePage(Output, DefaultSystemCodePage);
    SetConsoleOutputCP(CP_UTF8);
    SetTextCodePage(Output, CP_UTF8);  // why 0 ??
    SetTextCodePage(Output, 0);  // why 0 ??
    GetConsoleMode(GetStdHandle(STD_OUTPUT_HANDLE), dwOriginalOutMode);
    dwRequestedOutModes := ENABLE_VIRTUAL_TERMINAL_PROCESSING;
    dwOutMode := dwOriginalOutMode OR dwRequestedOutModes;
    SetConsoleMode(GetStdHandle(STD_OUTPUT_HANDLE), dwOutMode);
  end;
  {$ENDIF}

// ---------------------------
// main program
// ---------------------------
var
  sql: string;
begin
  ConfigFileName := ChangeFileExt(ParamStr(0), '.save');

  // set global max_allowed_packet=2048
  // set global net_buffer_length=2048

  MariaDbDebug := False;

  {$IFDEF WINDOWS}
  InitWindows;
  {$ENDIF}

  writeln(clrBold + 'Welcome to the MariaDB connector.' + clrNormal);
  writeln('');
  writeln('Type ?, help or \h for help.');
  writeln('');

  MDB := TMariaDBConnector.Create;
  try
    try

      loadconfig;
      EnterOrCheckCredentialsAndConnect(False);

      repeat

        if MDB.Connected then Database := GetSQLValue('SELECT DATABASE();');
        Write(clrGreen + format('[%s] # ', [Database]) + clrNormal);

        readln(sql);

        if (sql = 'quit') then sql := '\q';
        if (sql = 'exit') then sql := '\q';
        if (sql = '?') then sql := '\h';
        if (sql = '\?') then sql := '\h';
        if (sql = 'help') then sql := '\h';
        if (sql = 'save') then sql := '\s';
        if (sql = 'unsave') then sql := '\u';
        if (sql = 'connect') then sql := '\c';
        if (sql = 'debug') then sql := '\d';
        if (sql = 'ping') then sql := '\p';
        if (sql = 'info') then sql := '\i';
        if (pos('test', sql) = 1) then sql := '\t1' + Copy(sql, 5);
        if (sql = 'untest') then sql := '\t2';

        if (sql = '\q') then break;
        if (sql = '\h') then printhelp;
        if (sql = '\s') then saveconfig;
        if (sql = '\u') then unsaveconfig;
        if (sql = '\d') then MariaDbDebug := not MariaDbDebug;
        if (sql = '\i') then printinfo;
        if (pos('\t1', sql) = 1) then createtestdatabase(sql);
        if (sql = '\t2') then cleanuptest;

        if (sql = '\c') then
        begin
          MDB.Quit; // also reset errors
          EnterOrCheckCredentialsAndConnect(True);
        end;

        if (sql = '\p') then
        begin
          if MDB.Ping then
            writeln('Ping response ok')
          else
            writeln('Error on ping');
        end;

        if (sql <> '') and (sql[1] <> '\') then
          if not DoSQL(sql) then;

      until False;

      if MDB.Connected then MDB.Quit;

    except
      on E: Exception do
        writeln('Exception: ' + E.Message);
    end;

  finally

    MDB.Free;
    writeln('Done');

  end;

end.
