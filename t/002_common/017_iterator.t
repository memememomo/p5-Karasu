use t::Utils;
use Mock::Basic;
use Test::More;

my $dbh = t::Utils->setup_dbh;
my $db = Mock::Basic->new({dbh => $dbh});
$db->setup_test_db;

$db->insert('mock_basic',{
    id   => 1,
    name => 'perl',
});
$db->insert('mock_basic',{
    id   => 2,
    name => 'ruby',
});

subtest 'all' => sub {
    my $itr = $db->search("mock_basic");
    my $rows = $itr->all;
    is ref $rows, 'ARRAY';
    is $rows->[0]->{id}, 1;
};

subtest 'iterator with no cache all/count' => sub {
    my $itr = $db->search("mock_basic");
    isa_ok $itr, 'Karasu::Iterator';

    my @rows = $itr->all;
    is scalar(@rows), 2, "rows count";

    ok !$itr->next, "cannot retrieve first row after count";
};

subtest 'iterator with no cache' => sub {
    my $itr = $db->search("mock_basic");
    isa_ok $itr, 'Karasu::Iterator';

    my $row1 = $itr->next;
    is ref $row1, 'HASH';
    my $row2 = $itr->next;
    is ref $row2, 'HASH';

    ok !$itr->next, 'no more row';
};

done_testing;
