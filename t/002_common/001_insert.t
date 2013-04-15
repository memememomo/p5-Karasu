use t::Utils;
use Mock::Basic;
use Test::More;

my $dbh = t::Utils->setup_dbh;
my $db = Mock::Basic->new({dbh => $dbh});
$db->setup_test_db;

subtest 'insert mock_basic data/ insert method' => sub {
    my $pk = $db->insert('mock_basic',{
        id   => 1,
        name => 'perl',
    });
    is $pk, 1;
};

subtest 'scalar ref' => sub {
    my $pk = $db->insert('mock_basic',{
        id   => 4,
        name => \"upper('c')",
    });
    is $pk, 4;
    is $db->single('mock_basic', {id => $pk})->{name}, 'C';
};

done_testing;
