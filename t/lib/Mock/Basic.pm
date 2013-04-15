package Mock::Basic;
use base qw(KarasuTest);


__PACKAGE__->load_plugin('Count');

sub create_sqlite {
    my ($class, $dbh) = @_;
    $dbh->do(q{
CREATE TABLE mock_basic (
  id   integer,
  name text,
  delete_fg int(1) default 0,
  primary key ( id )
});
    $dbh->do(q{
CREATE TABLE mock_basic_camelcase (
  Id   integer,
  Name text,
  DeleteFg int(1) default 0,
  primary key ( Id )
});
}

sub create_mysql {
    my ($class, $dbh) = @_;
    $dbh->do( q{DROP TABLE IF EXISTS mock_basic} );
    $dbh->do(q{
CREATE TABLE mock_basic (
  id   INT auto_increment,
  name TEXT,
  delete_fg TINYINT(1) default 0,
  PRIMARY KEY (id)
) ENGINE=InnoDB
});
}

sub create_pg {
    my ($class, $dbh) = @_;
    $dbh->do( q{DROP TABLE IF EXISTS mock_basic});
    $dbh->do(q{
CREATE TABLE mock_basic (
  id   SERIAL PRIMARY KEY,
  name TEXT,
  delete_fg boolean
)
});
}

1;

