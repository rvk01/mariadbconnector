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

var
  ConfigFileName: string = 'mycredentials.json'; // will be used for credentials
  Server: string;
  User: string;
  Password: string;
  Database: string;

  // ---------------------------
  // Examples
  // ---------------------------

var
  MDB: TMariaDBConnector;
  History: array of string;
  F: TField;

  function GetSQLValue(SQL: string; Column: integer = 1): string;
  begin
    Result := '';
    if MDB.Query(SQL) and MDB.Dataset.Active then
      if (MDB.Dataset.RecordCount > 0) and (Column <= MDB.Dataset.Fields.Count) then
        Result := MDB.Dataset.Fields[Column - 1].AsString;
  end;

  function DoSQL(SQL: string): boolean;
  var
    fmt: string;
    i: integer;
    s, h1, h2: string;
    ColumnFormat: boolean;
  begin
    Result := MDB.Query(SQL);
    if not Result then writeln('Error: ', SQL, ' ', MDB.LastError, ' ', MDB.LastErrorDesc);


    if MDB.Dataset.Active then
    begin

      ColumnFormat := True;
      for i := 0 to MDB.Dataset.Fields.Count - 1 do
        if MDB.MaxColumnLength[i] > 255 then ColumnFormat := False;

      for i := 0 to MDB.Dataset.Fields.Count - 1 do
        if MDB.Dataset.Fields[i].Alignment = taLeftJustify then MDB.MaxColumnLength[i] := -MDB.MaxColumnLength[i];

      h1 := '+';
      for i := 0 to MDB.Dataset.Fields.Count - 1 do h1 := h1 + StringOfChar('-', Abs(MDB.MaxColumnLength[i]) + 2) + '+';

      h2 := '|';
      for i := 0 to MDB.Dataset.Fields.Count - 1 do
      begin
        h2 := h2 + format(' %' + MDB.MaxColumnLength[i].ToString + 's |', [MDB.Dataset.Fields[i].FieldName]);
      end;

      if ColumnFormat then
      begin
        writeln(h1);
        writeln(h2);
        writeln(h1);
      end;

      MDB.Dataset.First;
      while not MDB.Dataset.EOF do
      begin

        if ColumnFormat then
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

          h2 := '';
          for i := 0 to MDB.Dataset.Fields.Count - 1 do
            if MDB.Dataset.Fields[i].AsString <> '' then
            begin
              h2 := MDB.Dataset.Fields[i].FieldName + ': ';
              if Abs(MDB.MaxColumnLength[i]) > 255 then h2 := h2 + #13#10;
              h2 := h2 + MDB.Dataset.Fields[i].AsString;
              writeln(h2);
            end;

        end;

        MDB.Dataset.Next;
      end;

      if ColumnFormat then
      begin
        writeln(h1);
        writeln(format('%d rows in set', [MDB.Dataset.RecordCount]));
      end;

    end;

  end;

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

  procedure printhistory(var sql: string);
  var
    I: integer;
  begin
    sql := '';
    for I := 0 to Length(History) - 1 do
      writeln(format('%.20s', [History[I]]));

  end;

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
    writeln('or `help upper` or `help format`.');
    writeln('');
  end;

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

  procedure unsaveconfig;
  begin
    SysUtils.DeleteFile(ConfigFileName);
    writeln('File with credentials deleted');
  end;

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

  procedure createtestdatabase(sql: string); // delete/create database and add records
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

  procedure cleanuptest;
  begin
    writeln('Dropping database connector_test');
    DoSQL('DROP DATABASE IF EXISTS connector_test;');
  end;

  procedure printinfo;
  var
    info: string;
    Up: string;
  begin
    Info := '';

    Up := GetSQLValue('show global status like ''Uptime'';', 2);
    Up := FormatDateTime('d" days" h" hours" n" minutes" s" seconds"', StrToInt(Up) / (24 * 60 * 60) + 1);

    Info := Info + 'Server version          ' + GetSQLValue('select @@version') + #13#10;
    Info := Info + 'Protocol version        ' + GetSQLValue('select @@protocol_version') + #13#10;
    Info := Info + 'Uptime                  ' + Up + #13#10;
    Info := Info + 'Current database        ' + Database + #13#10;
    Info := Info + 'Current user            ' + User + #13#10;
    Info := Info + 'Compression             ' + GetSQLValue('show global status like ''Compression'';', 2) + #13#10;
    Info := Info + 'Server characterset     ' + GetSQLValue('select @@character_set_server') + #13#10;
    Info := Info + 'Db     characterset     ' + GetSQLValue('select @@character_set_database') + #13#10;
    Info := Info + 'Client characterset     ' + GetSQLValue('select @@character_set_client') + #13#10;
    Info := Info + 'Conn.  characterset     ' + GetSQLValue('select @@character_set_connection') + #13#10;
    Info := Info + 'autocommit              ' + GetSQLValue('select @@autocommit') + #13#10;
    Info := Info + 'have_compress           ' + GetSQLValue('select @@have_compress') + #13#10;
    Info := Info + 'have_openssl            ' + GetSQLValue('select @@have_openssl') + #13#10;
    Info := Info + 'have_ssl                ' + GetSQLValue('select @@have_ssl') + #13#10;
    Info := Info + 'hostname                ' + GetSQLValue('select @@hostname') + #13#10;
    Info := Info + 'net_buffer_length       ' + GetSQLValue('select @@net_buffer_length') + #13#10;
    Info := Info + 'max_allowed_packet      ' + GetSQLValue('select @@max_allowed_packet') + #13#10;

    writeln(Info);

  end;

var
  sql: string;
begin
  ConfigFileName := ChangeFileExt(ParamStr(0), '.save');

  History := nil;
  Insert('history', History, length(History));
  Insert('help', History, length(History));
  Insert('ping', History, length(History));
  Insert('set global max_allowed_packet=2048', History, length(History));
  Insert('set global net_buffer_length=2048', History, length(History));
  Insert('select @@net_buffer_length, @@max_allowed_packet;', History, length(History));
  Insert('show databases', History, length(History));
  Insert('show tables', History, length(History));
  Insert('use connector_test', History, length(History));
  Insert('select * from test where id>=5 and id<=11', History, length(History));
  Insert('select * from test order by id desc', History, length(History));

  MariaDbDebug := False;

  // SetTextCodePage(Output, DefaultSystemCodePage);
  {$IFDEF WINDOWS}
  SetConsoleOutputCP(CP_UTF8);
  SetTextCodePage(Output, CP_UTF8);  // why 0 ??
  SetTextCodePage(Output, 0);  // why 0 ??
  {$ENDIF}

  writeln('Welcome to the MariaDB connector.');
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
        Write(format('[%s] # ', [Database]));

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
