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

subtest 'update mock_basic data' => sub {
    ok $db->update('mock_basic',{name => 'python'},{id => 1});
    my $row = $db->single('mock_basic',{id => 1});

    is ref $row, 'HASH';
    is $row->{name}, 'python';
};

subtest 'update row count' => sub {
    $db->insert('mock_basic',{
        id   => 2,
        name => 'c++',
    });

    my $cnt = $db->update('mock_basic',{name => 'java'});
    is $cnt, 2;
};

done_testing;
