use strict;
use warnings;
use t::Utils;
use Test::More;

{
    package Mock::BasicALLINONE;
    use parent 'Karasu';

    sub setup_test_db {
        shift->do(q{
CREATE TABLE mock_basic (
  id   integer,
  name text,
  delete_fg int(1) default 0,
  primary key ( id )
)
});
    }
}

my $db = Mock::BasicALLINONE->new(connect_info => ['dbi:SQLite::memory:', '', '']);

$db->setup_test_db;
$db->insert('mock_basic', {
    id => 1,
    name => 'perl',
});

my $itr = $db->search_by_sql(q{SELECT * FROM mock_basic WHERE id = ?}, [1]);
isa_ok $itr, 'Karasu::Iterator';

my $row = $itr->next;
is ref $row, 'HASH';
is $row->{id}, 1;
is $row->{name}, 'perl';

done_testing;
