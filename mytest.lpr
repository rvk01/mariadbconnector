program mytest;

{$mode ObjFPC}{$H+}

uses
  umariadbconnector,
  DB,
  {$IFDEF UNIX}
    {$IFDEF UseCThreads}
    cthreads,
    {$ENDIF}
  {Widestring manager needed for widestring support}
  cwstring,
  {$ENDIF}
  {$IFDEF WINDOWS}
    Windows, {for setconsoleoutputcp}
  {$ENDIF}
  SysUtils, JsonConf;

const
  ConfigFileName = 'mycredentials.json'; // will be used for credentials
  Server: String = 'localhost';
  User: String = 'root';
  Password: String = 'password';
  Database: String = ''; // can be empty

  // ---------------------------
  // Examples
  // ---------------------------

var
  MDB: TMariaDBConnector;
  F: TField;

  function DoSQL(SQL: string): boolean;
  begin
    Result := MDB.Query(SQL);
    if not Result then writeln('Error: ', SQL, ' ', MDB.LastError, ' ', MDB.LastErrorDesc);

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
    for i := 1 to len do
    begin
      c := chars[Random(Length(chars))];
      if l + Length(c) > Length(Result) then
        SetLength(Result, Length(Result) * 2);
      Move(c[1], Result[l + 1], Length(c));
      Inc(l, Length(c));
    end;
    SetLength(Result, l);
  end;

  procedure GetCredentials;
  var
    Conf: TJsonConfig;
  begin
    Conf := TJSONConfig.Create(nil);
    try
      Conf.Formatted := True;
      Conf.FileName := ConfigFileName;
      if not FileExists(ConfigFileName) then
      begin
        Conf.SetValue('Server', Server);
        Conf.SetValue('User', User);
        Conf.SetValue('Password', Password);
        Conf.SetValue('Database', Database);
      end;
      Server := string(Conf.GetValue('Server', Server));
      User := string(Conf.GetValue('User', User));
      Password := string(Conf.GetValue('Password', Password));
      Database := string(Conf.GetValue('Database', Database));
    finally
      Conf.Free;
    end;
  end;

var
  sql: string;
  i: integer;
begin

  GetCredentials;

  MariaDbDebug := false;

  // SetTextCodePage(Output, DefaultSystemCodePage);
  {$IFDEF WINDOWS}
  SetConsoleOutputCP(CP_UTF8);
  SetTextCodePage(Output, CP_UTF8);  // why 0 ??
  SetTextCodePage(Output, 0);  // why 0 ??
  {$ENDIF}

  MDB := TMariaDBConnector.Create;
  try

    if not MDB.ConnectAndLogin(Server, '', User, Password, Database) then
    begin
      writeln('Problem connecting. ', MDB.LastError, ' ', MDB.LastErrorDesc);
      exit;
    end;

    Writeln('We have a connection');

    {
    if MDB.Ping then
      writeln('Ping response ok')
    else
      writeln('Error on ping');
    MDB.SetMultiOptions(true);
    if MDB.Query('use cis') then;
    // if MDB.Query('use cis; use mysql;') then;
    }

    DoSQL('DROP DATABASE IF EXISTS connector_test;');
    DoSQL('CREATE DATABASE connector_test');
    DoSQL('use connector_test');
    sql := 'create table if not exists test (';
    sql := sql + ' id bigint auto_increment primary key,';
    sql := sql + ' name varchar(20) charset utf8,';
    sql := sql + ' key name (name(5))';
    sql := sql + ') engine=InnoDB default charset latin1;';
    DoSQL(sql);

    // insert 26 records
    for i := 1 to 26 do
    begin
      sql := rand;
      // writeln((sql));
      // writeln(Buf2Hex(sql));
      DoSQL('INSERT INTO test (name) VALUES (''' + sql + ''');');
    end;


    {

    TEST MULTI COMMANDS - doesn't work yet

    sql := '';
    for i := 1 to 26 do DoSQL('INSERT INTO test (name) VALUES (''' + rand + ''');');
    writeln('----------');
    writeln(sql);
    writeln('----------');
    MariaDbDebug := True;
    MDB.SetMultiOptions(true);
    DoSQL(sql);
    }

    sql := 'select * from test where id>=5 and id<=11';
    writeln(sql);
    if MDB.Query(sql) then
    begin
      if MDB.Dataset.Active then
      begin
        MDB.Dataset.First;
        while not MDB.Dataset.EOF do
        begin
          for F in MDB.Dataset.Fields do Write(format('%s  '#9' len:%d '#9#9, [F.AsString, Length(F.AsString)]));
          writeln;
          MDB.Dataset.Next;
        end;
      end;
    end
    else
      writeln('Error doing select: ', MDB.LastError, ' ', MDB.LastErrorDesc);

    DoSQL('DROP TABLE IF EXISTS test;');
    DoSQL('DROP DATABASE IF EXISTS connector_test;');

    MDB.Quit;

  finally

    MDB.Free;
    writeln('Done, press enter to exit');
    readln;

  end;

end.
