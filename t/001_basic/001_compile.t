use t::Utils;
use Test::More;

BEGIN { use_ok( 'Mock::Basic' ); }

isa_ok 'Mock::Basic', 'Karasu';

use DBD::SQLite;
diag('DBD::SQLite version is '.$DBD::SQLite::VERSION);

done_testing;
