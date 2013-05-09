package Karasu;
use strict;
use warnings;
use Carp ();
use Class::Load ();
use DBI 1.33;
use DBIx::TransactionManager 1.06;
use Karasu::Iterator;
use Karasu::QueryBuilder;
use Class::Accessor::Lite
    rw => [ qw(
                  connect_info
                  on_connect_do
                  sql_builder
                  sql_comment
                  owner_pid
                  no_ping
                  fields_case
          )];

our $VERSION = '0.01';


sub load_plugin {
    my ($class, $pkg, $opt) = @_;
    $pkg = $pkg =~ s/^\+// ? $pkg : "Karasu::Plugin::$pkg";
    Class::Load::load_class($pkg);

    $class = ref($class) if ref($class);

    my $alias = delete $opt->{alias} || +{};
    {
        no strict 'refs';
        for my $method ( @{"${pkg}::EXPORT"} ) {
            *{$class . '::' . ($alias->{$method} || $method)} = $pkg->can($method);
        }
    }

    $pkg->init($class, $opt) if $pkg->can('init');
}

sub new {
    my $class = shift;
    my %args = @_ == 1 ? %{$_[0]} : @_;
    my $loader = delete $args{loader};

    my $self = bless {
        owner_pid   => $$,
        no_ping     => 0,
        fields_case => 'NAME_lc',
        %args,
    }, $class;

    unless ($self->connect_info || $self->{dbh}) {
        Carp::croak("'dbh' or 'connect_info' is required.");
    }

    if ( ! $self->{dbh} ) {
        $self->connect;
    } else {
        $self->_prepare_from_dbh;
    }

    return $self;
}

# forcefully connect
sub connect {
    my ($self, @args) = @_;

    $self->in_transaction_check;

    if (@args) {
        $self->connect_info( \@args );
    }
    my $connect_info = $self->connect_info;
    $connect_info->[3] = {
        # basic defaults
        AutoCommit => 1,
        PrintError => 0,
        RaiseError => 1,
        %{ $connect_info->[3] || {} },
    };

    $self->{dbh} = eval { DBI->connect(@$connect_info) }
        or Carp::croak("Connection error: " . ($@ || $DBI::errstr));
    delete $self->{txn_manager};

    $self->owner_pid($$);

    $self->_on_connect_do;
    $self->_prepare_from_dbh;
}

sub _on_connect_do {
    my $self = shift;

    if ( my $on_connect_do = $self->on_connect_do ) {
        if (not ref($on_connect_do)) {
            $self->do($on_connect_do);
        } elsif (ref($on_connect_do) eq 'CODE') {
            $on_connect_do->($self);
        } elsif (ref($on_connect_do) eq 'ARRAY') {
            $self->do($_) for @$on_connect_do;
        } else {
            Carp::croak('Invalid on_connect_do: '.ref($on_connect_do));
        }
    }
}

sub reconnect {
    my $self = shift;

    $self->in_transaction_check;

    my $dbh = $self->{dbh};

    $self->disconnect();

    if ( @_ ) {
        $self->connect(@_);
    }
    else {
        # Why don't use $dbh->clone({InactiveDestroy => 0}) ?
        # because, DBI v1.616 clone with \%attr has bug.
        # my $dbh2 = $dbh->clone({});
        # my $dbh3 = $dbh2->clone({});
        # $dbh2 is ok, but $dbh3 is undef.
        # ---
        # Don't assign $self->{dbh} directry
        # Because if $self->{dbh} is undef then reconnect fail always.
        # https://github.com/nekokak/p5-Teng/pull/98
        my $new_dbh = eval { $dbh->clone }
            or Carp::croak("ReConnection error: " . ($@ || $DBI::errstr));
        $self->{dbh} = $new_dbh;
        $self->{dbh}->{InactiveDestroy} = 0;

        $self->owner_pid($$);
        $self->_on_connect_do;
        $self->_prepare_from_dbh;
    }
}

sub disconnect {
    my $self = shift;

    delete $self->{txn_manager};
    if ( my $dbh = $self->{dbh} ) {
        if ( $self->owner_pid && ($self->owner_pid != $$) ) {
            $dbh->{InactiveDestroy} = 1;
        }
        else {
            $dbh->disconnect;
        }
    }
    $self->owner_pid(undef);
}

sub _prepare_from_dbh {
    my $self = shift;

    $self->{driver_name} = $self->{dbh}->{Driver}->{Name};
    my $builder = $self->{sql_builder};
    if (! $builder ) {
        # XXX Hackish
        $builder = Karasu::QueryBuilder->new(driver => $self->{driver_name} );
        $self->sql_builder( $builder );
    }
    $self->{dbh}->{FetchHashKeyName} = $self->{fields_case};

    # $self->{schema}->prepare_from_dbh($self->{dbh}) if $self->{schema};
}

sub _verify_pid {
    my $self = shift;

    if ( !$self->owner_pid || $self->owner_pid != $$ ) {
        $self->reconnect;
    }
    elsif ( my $dbh = $self->{dbh} ) {
        if ( !$dbh->FETCH('Active') ) {
            $self->reconnect;
        }
        elsif ( !$self->no_ping && !$dbh->ping) {
            $self->reconnect;
        }
    }
}

sub dbh {
    my $self = shift;

    $self->_verify_pid;
    $self->{dbh};
}

sub connected {
    my $self = shift;
    my $dbh = $self->{dbh};
    return $self->owner_pid && $dbh->ping;
}

our $SQL_COMMENT_LEVEL = 0;
sub execute {
    my ($self, $sql, $binds) = @_;

    if ($ENV{KARASU_SQL_COMMENT} || $self->sql_comment) {
        my $i = $SQL_COMMENT_LEVEL; # optimize, as we would *NERVER* be called
        while ( my (@caller) = caller($i++) ) {
            next if ( $caller[0]->isa( __PACKAGE__ ) );
            my $comment = "$caller[1] at line $caller[2]";
            $comment =~ s/\*\// /g;
            $sql = "/* $comment */\n$sql";
            last;
        }
    }

    my $sth;
    eval {
        $sth = $self->dbh->prepare($sql);
        my $i = 1;
        for my $v ( @{ $binds || [] } ) {
            $sth->bind_param( $i++, ref($v) eq 'ARRAY' ? @$v : $v );
        }
        $sth->execute();
    };

    if ($@) {
        $self->handle_error($sql, $binds, $@);
    }

    return $sth;
}

sub _last_insert_id {
    my ($self, $table_name) = @_;

    my $driver = $self->{driver_name};
    if ( $driver eq 'mysql' ) {
        return $self->dbh->{mysql_insertid};
    } elsif ( $driver eq 'Pg' ) {
        return $self->dbh->last_insert_id( undef, undef, undef, undef,{ sequence => join( '_', $table_name, 'id', 'seq' ) } );
    } elsif ( $driver eq 'SQLite' ) {
        return $self->dbh->func('last_insert_rowid');
    } elsif ( $driver eq 'Oracle' ) {
        return;
    } else {
        Carp::croak "Don't know how to get last insert id for $driver";
    }
}

sub do_insert {
    my ($self, $table_name, $args, $prefix) = @_;

    $prefix ||= 'INSERT INTO';
    my ($sql, @binds) = $self->{sql_builder}->insert( $table_name, $args, { prefix => $prefix } );
    $self->execute($sql, \@binds);
}

sub insert {
    my ($self, $table_name, $args, $prefix) = @_;

    $self->do_insert($table_name, $args, $prefix);
    $self->_last_insert_id($table_name);
}

sub bulk_insert {
    my ($self, $table_name, $args) = @_;

    return unless scalar(@{$args||[]});

    my $dbh = $self->dbh;
    my $can_multi_insert = $dbh->{Driver}->{Name} eq 'mysql' ? 1
                         : $dbh->{Driver}->{Name} eq 'Pg'
                             && $dbh->{ pg_server_version } >= 82000 ? 1
                         : 0;

    if ($can_multi_insert) {
        my ($sql, @binds) = $self->sql_builder->insert_multi( $table_name, $args );
        $self->execute($sql, \@binds);
    } else {
        # use transaction for better performance and atomicity
        my $txn = $self->txn_scope();
        for my $arg (@$args) {
            # do not run trigger for consistency with mysql.
            $self->insert($table_name, $arg);
        }
        $txn->commit;
    }
}

sub update {
    my ($self, $table_name, $args, $where) = @_;

    my ($sql, @binds) = $self->{sql_builder}->update( $table_name, $args, $where );
    my $sth = $self->execute($sql, \@binds);
    my $rows = $sth->rows;
    $sth->finish;

    $rows;
}

sub delete {
    my ($self, $table_name, $where) = @_;

    my ($sql, @binds) = $self->{sql_builder}->delete( $table_name, $where );
    my $sth = $self->execute($sql, \@binds);
    my $rows = $sth->rows;
    $sth->finish;

    $rows;
}


#--------------------------------------------------------------------------------
# for transaction
sub txn_manager {
    my $self = shift;
    $self->_verify_pid;
    $self->{txn_manager} ||= DBIx::TransactionManager->new($self->dbh);
}

sub in_transaction_check {
    my $self = shift;

    return unless $self->{txn_manager};

    if ( my $info = $self->{txn_manager}->in_transaction ) {
        my $caller = $info->{caller};
        my $pid    = $info->{pid};
        Carp::confess("Detected transaction during a connect operation (last known transaction at $caller->[1] line $caller->[2], pid $pid). Refusing to proceed at");
    }
}

sub txn_scope {
    my $self = shift;
    my @caller = caller();

    $self->txn_manager->txn_scope(caller => \@caller);
}

sub txn_begin {
    my $self = shift;

    $self->txn_manager->txn_begin;
}
sub txn_rollback { $_[0]->txn_manager->txn_rollback }
sub txn_commit   { $_[0]->txn_manager->txn_commit   }
sub txn_end      { $_[0]->txn_manager->txn_end      }

#--------------------------------------------------------------------------------

sub do {
    my ($self, $sql, $attr, @bind_vars) = @_;
    my $ret;
    eval { $ret = $self->dbh->do($sql, $attr, @bind_vars) };
    if ($@) {
        $self->handle_error($sql, @bind_vars ? \@bind_vars : '', $@);
    }
    $ret;
}

sub _get_select_columns {
    my ($self, $opt) = @_;

    return $opt->{'+columns'}
        ? ['*', @{$opt->{'+columns'}}]
        : ($opt->{columns} || ['*'])
    ;
}

sub search {
    my ($self, $table_name, $where, $opt) = @_;

    my ($sql, @binds) = $self->{sql_builder}->select(
        $table_name,
        $self->_get_select_columns($opt),
        $where,
        $opt
    );

    $self->search_by_sql($sql, \@binds, $table_name);
}

sub _bind_named {
    my ($self, $sql, $args) = @_;

    my @bind;
    $sql =~ s{:([A-Za-z_][A-Za-z0-9_]*)}{
        Carp::croak("'$1' does not exist in bind hash") if !exists $args->{$1};
        if ( ref $args->{$1} && ref $args->{$1} eq "ARRAY" ) {
            push @bind, @{ $args->{$1} };
            my $tmp = join ',', map { '?' } @{ $args->{$1} };
            "( $tmp )";
        } else {
            push @bind, $args->{$1};
            '?'
        }
    }ge;

    return ($sql, \@bind);
}

sub search_named {
    my ($self, $sql, $args, $table_name) = @_;

    $self->search_by_sql($self->_bind_named($sql, $args), $table_name);
}

sub single {
    my ($self, $table_name, $where, $opt) = @_;

    $opt->{limit} = 1;

    my ($sql, @binds) = $self->{sql_builder}->select(
        $table_name,
        $self->_get_select_columns($opt),
        $where,
        $opt
    );
    my $sth = $self->execute($sql, \@binds);
    my $row = $sth->fetchrow_hashref($self->{fields_case});

    return $row ? $row : undef;
}

sub search_by_sql {
    my ($self, $sql, $bind, $table_name) = @_;

    my $sth = $self->execute($sql, $bind);
    my $itr = Karasu::Iterator->new(
        karasu     => $self,
        sth        => $sth,
        sql        => $sql,
        table_name => $table_name
    );
    return wantarray ? $itr->all : $itr;
}

sub single_by_sql {
    my ($self, $sql, $bind, $table_name) = @_;

    my $sth = $self->execute($sql, $bind);
    my $row = $sth->fetchrow_hashref($self->{fields_case});

    return $row ? $row : undef;
}

sub single_named {
    my ($self, $sql, $args, $table_name) = @_;

    $self->single_by_sql($self->_bind_named($sql, $args), $table_name);
}

sub _guess_table_name {
    my ($class, $sql) = @_;

    if ($sql =~ /\sfrom\s+["`]?([\w]+)["`]?\s*/si) {
        return $1;
    }
    return;
}

sub handle_error {
    my ($self, $stmt, $bind, $reason) = @_;
    require Data::Dumper;

    local $Data::Dumper::Maxdepth = 2;
    $stmt =~ s/\n/\n          /gm;
    Carp::croak sprintf <<"TRACE", $reason, $stmt, Data::Dumper::Dumper($bind);
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
@@@@@ Karasu 's Exception @@@@@
Reason  : %s
SQL     : %s
BIND    : %s
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
TRACE
}

sub DESTROY {
    my $self = shift;

    if ( $self->owner_pid and $self->owner_pid != $$ and my $dbh = $self->{dbh} ) {
        $dbh->{InactiveDestroy} = 1;
    }
}


1;

__END__
=head1 NAME

Karasu - very simple DBI wrapper

=head1 SYNOPSIS

    my $db = MyDB->new({ connect_info => [ 'dbi:SQLite:' ] });
    my $id = $db->insert( 'table' => {
        col1 => $value
    } );

=head1 DESCRIPTION

Karasu is very simple DBI wrapper.
It aims to be lightweight, with minimal dependencies so it's easier to install.

=head1 BASIC USAGE

create your db model base class.

    package Your::Model;
    use parent 'Karasu';
    1;

    use Your::Model;

    my $karasu = Your::Model->new(\%args);
    # insert new record.
    my $id = $karasu->insert('user',
        {
            id   => 1,
        }
    );
    $karasu->update('user', {name => 'memememomo'}, {id => $id});

    $row = $karasu->single_by_sql(q{SELECT id, name FROM user WHERE id = ?}, [ 1 ]);
    $karasu->delete('user', {id => $row->{id}});

=head1 ARCHITECTURE

Karasu classes are comprised of one distinct component:

=head2 MODEL

The C<model> is where you say

    package MyApp::Model;
    use parent 'Karasu';

This is the entry point to using Karasu. You connect, insert, update, delete, select stuff using this object.


=head1 METHODS

Karasu provides a number of methods to all your classes,

=over

=item $karasu = Karasu->new(\%args)

Creates a new Karasu instance.

    # connect new database connection.
    my $db = Your::Model->new(
        connect_info => [ $dsn, $username, $password, \%connect_options ]
    );

Arguments can be:

=over

=item * C<connect_info>

Specifies the information required to connect to the database.
The argument should be a reference to a array in the form:

    [ $dsn, $user, $password, \%options ]

You must pass C<connect_info> or C<dbh> to the constructor.

=item * C<dbh>

Specifies the database handle to use.

=item * C<fields_case>

specific DBI.pm's FetchHashKeyName.

=item * C<sql_builder>

Speficies the SQL builder object. By default SQL::Maker is used, and as such,
if you provide your own SQL builder the interface needs to be compatible
with SQL::Maker.

=back

=item $id = $karasu->insert($table_name, \%row_data)

Inserts a new record. Returns the primary key.

    my $id = $karasu->insert('user',{
        id   => 1,
        name => 'uchico',
    });


=item $karasu->bulk_insert($table_name, \@rows_data)

Accepts either an arrayref of hashrefs.
each hashref should be a structure suitable
forsubmitting to a Your::Model->insert(...) method.

insert many record by bulk.

example:

    Your::Model->bulk_insert('user',[
        {
            id   => 1,
            name => 'nyaruko',
        },
        {
            id   => 2,
            name => 'kuuko',
        },
        {
            id   => 3,
            name => 'hasuta',
        },
    ]);

=item $update_row_count = $karasu->update($table_name, \%update_row_data, [\%update_condition])

Calls UPDATE on C<$table_name>, with values specified in C<%update_ro_data>, and returns the number of rows updated. You may optionally specify C<%update_condition> to create a conditional update query.

    my $update_row_count = $karasu->update('user',
        {
            name => 'uchico',
        },
        {
            id => 1
        }
    );
    # Executes UPDATE user SET name = 'uchico' WHERE id = 1


=item $delete_row_count = $karasu->delete($table, \%delete_condition)

Deletes the specified record(s) from C<$table> and returns the number of rows deleted. You may optionally specify C<%delete_condition> to create a conditional delete query.

    my $rows_deleted = $karasu->delete( 'user', {
        id => 1
    } );
    # Executes DELETE FROM user WHERE id = 1


=item $itr = $karasu->search($table_name, [\%search_condition, [\%search_attr]])

simple search method.
search method get Karasu::Iterator's instance object.

see L<Karasu::Iterator>

get iterator:

    my $itr = $karasu->search('user',{id => 1},{order_by => 'id'});

get rows:

    my @rows = $karasu->search('user',{id => 1},{order_by => 'id'});

=item $row = $karasu->single($table_name, \%search_condition)

get one record.
give back one case of the beginning when it is acquired plural records by single method.

    my $row = $karasu->single('user',{id =>1});

=item $itr = $karasu->search_named($sql, [\%bind_values, [$table_name]])

execute named query

    my $itr = $karasu->search_named(q{SELECT * FROM user WHERE id = :id}, {id => 1});

If you give ArrayRef to value, that is expanded to "(?,?,?,?)" in SQL.
It's useful in case use IN statement.

    # SELECT * FROM user WHERE id IN (?,?,?);
    # bind [1,2,3]
    my $itr = $karasu->search_named(q{SELECT * FROM user WHERE id IN :ids}, {ids => [1, 2, 3]});

=item $itr = $karasu->search_by_sql($sql, [\@bind_values])

execute your SQL

    my $itr = $karasu->search_by_sql(q{
        SELECT
            id, name
        FROM
            user
        WHERE
            id = ?
    },[ 1 ]);


=item $row = $karasu->single_by_sql($sql, [\@bind_values, [$table_name]])

get one record from your SQL.

    my $row = $karasu->single_by_sql(q{SELECT id,name FROM user WHERE id = ? LIMIT 1}, [1]);

This is a shortcut for

    my $row = $karasu->search_by_sql(q{SELECT id,name FROM user WHERE id = ? LIMIT 1}, [1])->next;

But optimized implementation.

=item $row = $karasu->single_named($sql, [\%bind_values])

get one record from execute named query

    my $row = $karasu->single_named(q{SELECT id,name FROM user WHERE id = :id LIMIT 1}, {id => 1}, 'user');

This is a shortcut for

    my $row = $karasu->search_named(q{SELECT id,name FROM user WHERE id = :id LIMIT 1}, {id => 1})->next;

But optimized implementation.

=item $sth = $karasu->execute($sql, [\@bind_values])

execute query and get statement handler.
and will be inserted caller's file and line as a comment in the SQL if $ENV{KARASU_SQL_COMMENT} or sql_comment is true value.

=item $karasu->txn_scope

Creates a new transaction scope guard object.

    do {
        my $txn = $karasu->txn_scope;

        $karasu->update('user', {foo => 'bar'});

        $txn->commit;
    }

If an exception occurs, or the guard object otherwise leaves the scope
before C<< $txn->commit >> is called, the transaction will be rolled
back by an explicit L</txn_rollback> call. In essence this is akin to
using a L</txn_begin>/L</txn_commit> pair, without having to worry
about calling L</txn_rollback> at the right places. Note that since there
is no defined code closure, there will be no retries and other magic upon
database disconnection.

=item $txn_manager = $karasu->txn_manager

Get the DBIx::TransactionManager instance.

=item $karasu->txn_begin

start new transaction.

=item $karasu->txn_commit

commit transaction.

=item $karasu->txn_rollback

rollback transaction.

=item $karasu->txn_end

finish transaction.

=item $karasu->do($sql, [\%option, @bind_values])

Execute the query specified by C<$sql>, using C<%option> and C<@bind_values> as necessary. This pretty much a wrapper around L<http://search.cpan.org/dist/DBI/DBI.pm#do>

=item $karasu->dbh

get database handle.

=item $karasu->connect(\@connect_info)

connect database handle.

connect_info is [$dsn, $user, $password, $options].

If you give \@connect_info, create new database connection.

=item $karasu->disconnect()

Disconnects from the currently connected database.

=item $karasu->load_plugin();

 $karasu->load_plugin($plugin_class, $options);

This imports plugin class's methods to C<$karasu> class
and it calls $plugin_class's init method if it has.

 $plugin_class->init($karasu, $options);

If you want to change imported method name, use C<alias> option.
for example:

 YourDB->load_plugin('BulkInsert', { alias => { bulk_insert => 'isnert_bulk' } });

BulkInsert's "bulk_insert" method is imported as "insert_bulk".

=item $karasu->handle_error

handling error method.

=item How do you use display the profiling result?

use L<Devel::KYTProf>.

=back

=head1 TRIGGERS

Karasu does not support triggers (NOTE: do not confuse it with SQL triggers - we're talking about Perl level triggers). If you really want to hook into the various methods, use something like L<Moose>, L<Mouse>, and L<Class::Method::Modifiers>.

=head1 Row Object, Inflate/Deflate, Table Schema

Karasu does not support Row Object, Inflate/Deflate, Table Schema. If you really want to use them, use L<Teng>.

=head1 SEE ALSO

=head2 Fork

This module was forked from L<Teng>, around version 0.18.
many incompatible changes have been made.

=head1 BUGS AND LIMITATIONS

No bugs have been reported.

=head1 AUTHORS

Uchico  C<< <memememomo __at__ gmail.com> >>

=head1 REPOSITORY

  git clone https://github.com/memememomo/p5-Karasu

=head1 LICENCE AND COPYRIGHT

Copyright (c) 2010, the Karasu L</AUTHOR>. All rights reserved.

This module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself. See L<perlartistic>.

=cut
