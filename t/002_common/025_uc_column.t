use t::Utils;
use Mock::Basic;
use Test::More;

my $dbh = t::Utils->setup_dbh();
my $db = Mock::Basic->new({dbh => $dbh, fields_case => 'NAME'});
$db->setup_test_db;

$db->insert('mock_basic_camelcase',{
    Id   => 1,
    Name => 'perl',
});

subtest 'single' => sub {
    my $row = $db->single('mock_basic_camelcase',{Id => 1});
    is ref $row, 'HASH';
    is $row->{Id}, 1;
    is $row->{Name}, 'perl';
    is_deeply $row, +{
        Id        => 1,
        Name      => 'perl',
        DeleteFg  => 0,
    };
};

subtest 'single' => sub {
    my $rows = [$db->search('mock_basic_camelcase')->all];
    is_deeply $rows, [+{
        Id        => 1,
        Name      => 'perl',
        DeleteFg  => 0,
    }];
};

done_testing;
