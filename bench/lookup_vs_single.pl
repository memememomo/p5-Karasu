#! /usr/bin/perl
use strict;
use warnings;
use Benchmark qw(:all :hireswallclock);
use Data::Dumper;
use Test::Mock::Guard qw/mock_guard/;

{
    package Bench;
    use parent 'Karasu';
    __PACKAGE__->load_plugin('Lookup');
}
my $gurad = mock_guard('DBI::st' => +{fetchrow_hashref => +{id => 1, name => 'nekokak', age => 33}});

my $db = Bench->new({connect_info => ['dbi:SQLite::memory:','','']});

$db->do( q{DROP TABLE IF EXISTS user} );
$db->do(q{
    CREATE TABLE user (
        id   INT PRIMARY KEY,
        name TEXT,
        age  INT
    );
});

my $row = $db->single('user', { id => 1 });

my $dbh = $db->dbh;

cmpthese(10000 => +{
    dbi             => sub {$dbh->selectrow_hashref('SELECT id,name,age FROM user where id = ?', undef, 1)},
    single          => sub {$db->single('user', +{id => 1})},
    single_by_sql   => sub {$db->single_by_sql('SELECT id,name,age FROM user WHERE id = ?', [1], 'user')},
    single_named   => sub {$db->single_named('SELECT id,name,age FROM user WHERE id = :id', {id => 1}, 'user')},
    lookup          => sub {$db->lookup('user', +{id => 1})},
    lookup_arrayref => sub {$db->lookup('user', [id => 1])},
}, 'all');

__END__

Benchmark: timing 10000 iterations of dbi, lookup, lookup_arrayref, single, single_by_sql, single_named...
    dbi: 0.479716 wallclock secs ( 0.47 usr  0.00 sys +  0.00 cusr  0.00 csys =  0.47 CPU) @ 21276.60/s (n=10000)
    lookup: 0.819557 wallclock secs ( 0.81 usr  0.00 sys +  0.00 cusr  0.00 csys =  0.81 CPU) @ 12345.68/s (n=10000)
lookup_arrayref: 0.82034 wallclock secs ( 0.81 usr  0.00 sys +  0.00 cusr  0.00 csys =  0.81 CPU) @ 12345.68/s (n=10000)
    single: 1.47863 wallclock secs ( 1.47 usr  0.00 sys +  0.00 cusr  0.00 csys =  1.47 CPU) @ 6802.72/s (n=10000)
single_by_sql: 0.599441 wallclock secs ( 0.60 usr  0.00 sys +  0.00 cusr  0.00 csys =  0.60 CPU) @ 16666.67/s (n=10000)
single_named: 0.718563 wallclock secs ( 0.70 usr  0.00 sys +  0.00 cusr  0.00 csys =  0.70 CPU) @ 14285.71/s (n=10000)
                   Rate single lookup_arrayref lookup single_named single_by_sql  dbi
single           6803/s     --            -45%   -45%         -52%          -59% -68%
lookup_arrayref 12346/s    81%              --    -0%         -14%          -26% -42%
lookup          12346/s    81%              0%     --         -14%          -26% -42%
single_named    14286/s   110%             16%    16%           --          -14% -33%
single_by_sql   16667/s   145%             35%    35%          17%            -- -22%
dbi             21277/s   213%             72%    72%          49%           28%   --
