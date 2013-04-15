use t::Utils;
use Mock::Basic;
use Test::More;

use File::Temp qw(tempdir);
my $tempdir = tempdir(CLEANUP => 1);
my $file    = File::Spec->catfile($tempdir, 'disconnect.db');

my $dbh = t::Utils->setup_dbh($file);
my $db = Mock::Basic->new({dbh => $dbh});
$db->setup_test_db;

subtest 'insert mock_basic data/ insert method' => sub {
    my $id = $db->insert('mock_basic',{
        id   => 1,
        name => 'perl',
    });
    is $id, 1;
};

subtest 'disconnect' => sub {
    $db->disconnect();
    ok ! $db->{dbh}->ping, "dbh is disconnected";
};

subtest 'insert after disconnect trigger a connect' => sub {
    my $db = Mock::Basic->new({dbh => t::Utils->setup_dbh($file)});
    my $id = $db->insert('mock_basic',{
        id   => 2,
        name => 'ruby',
    });
    is $id, 2;
};

done_testing;
