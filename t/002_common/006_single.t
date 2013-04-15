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

subtest 'single' => sub {
    my $row = $db->single('mock_basic',{id => 1});
    is ref $row, 'HASH';
    is $row->{id}, 1;
    is $row->{name}, 'perl';
    is_deeply $row, +{
        id        => 1,
        name      => 'perl',
        delete_fg => 0,
    };
};

subtest 'single / specific column' => sub {
    my $row = $db->single('mock_basic',{id => 1},+{columns => [qw/id/]});
    is ref $row, 'HASH';
    is $row->{id}, 1;
    is_deeply $row, +{
        id   => 1,
    };
};

subtest 'single / specific +column' => sub {
    my $row = $db->single('mock_basic',{id => 1},+{'+columns' => [\'id+20 as calc']});
    is ref $row, 'HASH';
    is $row->{id}, 1;
    is_deeply $row, +{
        id        => 1,
        name      => 'perl',
        delete_fg => 0,
        calc      => 21,
    };
};

done_testing;
