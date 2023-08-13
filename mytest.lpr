program mytest;

{$mode ObjFPC}{$H+}
{$APPTYPE CONSOLE}
{$codepage utf8}

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
  Server: rawbytestring = 'localhost';
  User: rawbytestring = 'root';
  Password: rawbytestring = 'password';
  Database: rawbytestring = ''; // can be empty

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
      Server := Conf.GetValue('Server', Server);
      User := Conf.GetValue('User', User);
      Password := Conf.GetValue('Password', Password);
      Database := Conf.GetValue('Database', Database);
    finally
      Conf.Free;
    end;
  end;

var
  sql: string;
  r: string;
  Name: string;
  i: integer;
begin

  GetCredentials;

  MariaDbDebug := false;

  // SetTextCodePage(Output, DefaultSystemCodePage);
  SetConsoleOutputCP(CP_UTF8);
  SetTextCodePage(Output, 0 { CP_UTF8 });  // why 0 ??

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
    sql := sql + '  a    bigint       auto_increment primary key,';
    sql := sql + '  name varchar(20) charset utf8,';
    sql := sql + '  key name (name(5))';
    sql := sql + ') engine=InnoDB default charset latin1;';
    DoSQL(sql);

    for i := 1 to 6 do
    begin
      Name := rand;
      // writeln(rand);
      DoSQL('INSERT INTO test (name) VALUES (''' + rand + ''');');
    end;

    {
    sql := '';
    for i := 1 to 3 do sql := sql + 'INSERT INTO test (name) VALUES (''aa''); ' + #13#10;
    writeln('----------');
    writeln(sql);
    writeln('----------');
    MariaDbDebug := True;
    MDB.SetMultiOptions(true);
    DoSQL(sql);
    }

    if MDB.Query('select * from test') then
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
