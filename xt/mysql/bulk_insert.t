use strict;
use warnings;
use utf8;
use xt::Utils::mysql;
use Test::More;
use lib './t';

use Karasu;

my $dbh = t::Utils->setup_dbh;
$dbh->do(q{
    create table user (
        user_id integer primary key
    );
});

{
    package Mock::DB;
    use parent 'Karasu';
}

my $db = Mock::DB->new(
    dbh => $dbh,
);

eval {
    $db->bulk_insert('user', );
};
ok not $@;

eval {
    $db->bulk_insert('user', []);
};
ok not $@;

my @ids = qw( 1 2 3 4 5 6 7 8 9 );
my @rows = map { +{ user_id => $_ } } @ids;
$db->bulk_insert('user', \@rows);

for my $id (@ids) {
    my $row = $db->single('user', { user_id => $id });
    is ($row->{user_id}, $id, "found: $id");
}

done_testing;
