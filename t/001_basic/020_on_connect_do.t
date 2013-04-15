use t::Utils;
use Test::More;

{
    package Mock::BasicOnConnectDo;
    our $CONNECTION_COUNTER;
    use parent qw/Karasu/;

    sub setup_test_db {
        shift->do(q{
CREATE TABLE mock_basic_on_connect_do (
  id   integer,
  name text,
  primary key ( id )
)
                  });
    }
}

subtest 'global level on_connect_do / coderef' => sub {
    local $Mock::BasicOnConnectDo::CONNECTION_COUNTER = 0;

    my $db = Mock::BasicOnConnectDo->new(
        {
            connect_info => [
                'dbi:SQLite:./t/main.db',
                '',
                '',
            ],
            on_connect_do => sub { $Mock::BasicOnConnectDo::CONNECTION_COUNTER++ }
        }
    );

    is($Mock::BasicOnConnectDo::CONNECTION_COUNTER, 1, "counter should called");
    $db->connect; # for do connection.
    is($Mock::BasicOnConnectDo::CONNECTION_COUNTER, 2, "called after connect");
    $db->reconnect; # for do connection.
    is($Mock::BasicOnConnectDo::CONNECTION_COUNTER, 3, "called after reconnect");
};

subtest 'instance level on_connect_do / coderef' => sub {
    my $counter = 0;
    my $db = Mock::BasicOnConnectDo->new(
        {
            connect_info => [
                'dbi:SQLite:./t/main.db',
                '',
                '',
            ],
            on_connect_do => sub { $counter++ },
        }
    );

    is($counter, 1, "counter should called");
    $db->connect; # for do connection.
    is($counter, 2, "called after connect");
    $db->reconnect; # for do connection.
    is($counter, 3, "called after reconnect");
};

subtest 'instance level on_connect_do / scalar' => sub {
    my $query;
    local *Mock::BasicOnConnectDo::do = sub {
        my ($self, $sql, ) = @_;
        $self->dbh; # ca be use dbh handler
        $query = $sql;
    };
    my $db = Mock::BasicOnConnectDo->new(
        +{
            connect_info => [
                'dbi:SQLite:./t/main.db',
                '',
                '',
            ],
            on_connect_do => 'select * from sqlite_master',
        }
    );

    is $query, 'select * from sqlite_master';
    $query='';
    $db->connect;
    is $query, 'select * from sqlite_master', 'called after connect';

    $query='';
    $db->reconnect;
    is $query, 'select * from sqlite_master', 'called after reconnect';
};

subtest 'instance level on_connect_do / array' => sub {
    my @query;
    local *Mock::BasicOnConnectDo::do = sub {
        my ($self, $sql, ) = @_;
        $self->dbh; # ca be use dbh handler
        push @query, $sql;
    };
    my $db = Mock::BasicOnConnectDo->new(
        {
            connect_info => [
                'dbi:SQLite:./t/main.db',
                '',
                '',
            ],
            on_connect_do => ['select * from sqlite_master', 'select * from sqlite_master'],
        }
    );

    is_deeply \@query, ['select * from sqlite_master', 'select * from sqlite_master'];
    @query = ();
    $db->connect;
    is_deeply \@query, ['select * from sqlite_master', 'select * from sqlite_master'], 'called after connect';

    @query = ();
    $db->reconnect;
    is_deeply \@query, ['select * from sqlite_master', 'select * from sqlite_master'], 'called after reconnect';
};

unlink './t/main.db';

done_testing();