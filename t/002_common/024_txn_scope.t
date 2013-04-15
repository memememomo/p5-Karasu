use t::Utils;
use Mock::Basic;
use Test::More;

my $dbh = t::Utils->setup_dbh;
my $db = Mock::Basic->new({dbh => $dbh});
$db->setup_test_db;

subtest 'insert using txn_scope' => sub {
    my $warning;
    local $SIG{__WARN__} = sub { $warning = $_[0] };
    {
        my $guard = $db->txn_scope();
        my $pk = $db->insert('mock_basic',{
            id   => 1,
            name => 'perl',
        });
        is $pk, 1;
        $guard->rollback;
    }

    if (! ok ! $warning, "no warnings received") {
        diag "got $warning";
    }
};

subtest 'insert using txn_scope (and let the guard fire)' => sub {
    my $warning;
    local $SIG{__WARN__} = sub { $warning = $_[0] };
    {
        my $guard = $db->txn_scope();
        my $pk = $db->insert('mock_basic',{
            id   => 1,
            name => 'perl',
        });
        is $pk, 1;
    }

    like $warning, qr{Guard created at \.?\/?t/002_common/024_txn_scope\.t line \d+};
};

done_testing;
