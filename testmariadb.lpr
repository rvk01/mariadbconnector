program testmariadb;

{$mode ObjFPC}{$H+}

uses
  umariadbconnector,
  DB;

const
  Server = '192.168.2.21';
  User: rawbytestring = 'root';
  Password: rawbytestring = 'password';
  Database: rawbytestring = ''; // can be empty

  // ---------------------------
  // Examples
  // ---------------------------

var
  MDB: TMariaDBConnector;
  F: TField;
begin

  MariaDbDebug := True;

  MDB := TMariaDBConnector.Create;
  try

    if MDB.ConnectAndLogin(Server, '', User, Password, Database) then
    begin

      Writeln('We have a connection');

      if MDB.Ping then
        writeln('Ping response ok')
      else
        writeln('Error on ping');

      if MDB.Query('show databases') then
      begin
        writeln;
        writeln('These are the databases');
        writeln('----------------------');
        MDB.Dataset.First;
        while not MDB.Dataset.EOF do
        begin
          for F in MDB.Dataset.Fields do Write(F.AsString, #9#9);
          writeln;
          MDB.Dataset.Next;
        end;
      end
      else
        writeln('Error showing databases: ', MDB.LastError, ' ', MDB.LastErrorDesc);

      if MDB.Query('use mysql') then
        writeln('We switched to mysql database')
      else
        writeln('Error switching to mysql database: ', MDB.LastError, ' ', MDB.LastErrorDesc);

      if MDB.Query('select host, user, password from user') then
      begin
        writeln;
        writeln('These are the users');
        writeln('----------------------');
        MDB.Dataset.First;
        while not MDB.Dataset.EOF do
        begin
          for F in MDB.Dataset.Fields do Write(F.AsString, #9#9);
          writeln;
          MDB.Dataset.Next;
        end;
      end
      else
        writeln('Error doing select: ', MDB.LastError, ' ', MDB.LastErrorDesc);


      MDB.Quit;

    end;

  finally

    MDB.Free;
    writeln('Done, press enter to exit');
    readln;

  end;

end.
