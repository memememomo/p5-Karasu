use t::Utils;
use Mock::Basic;
use Test::More;

my $dbh = t::Utils->setup_dbh;
my $db = Mock::Basic->new({dbh => $dbh});
$db->setup_test_db;
Mock::Basic->load_plugin('Replace');

subtest 'replace mock_basic data' => sub {
    my $pk = $db->insert('mock_basic',{
        id   => 1,
        name => 'perl',
    });
    is $pk, 1;


    $pk = $db->replace('mock_basic',{
        id   => 1,
        name => 'ruby',
    });
    is $pk, 1;

    my $replaced_row = $db->single('mock_basic', {
        id => $pk,
    });
    is $replaced_row->{name}, 'ruby';
};

done_testing;
