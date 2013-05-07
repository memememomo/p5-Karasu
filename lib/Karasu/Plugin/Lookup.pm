package Karasu::Plugin::Lookup;
use strict;
use warnings;
use utf8;

our @EXPORT = qw/lookup/;

sub lookup {
    my ($self, $table_name, $where, $opt) = @_;

    my (@keys, $values);
    if ( ref $where eq 'ARRAY' ) {
        my @w = @$where;
        while (my ($key, $val) = splice @w, 0, 2) {
            push @keys, $key;
            push @$values, $val;
        }
    }
    else {
        @keys = sort keys %$where;
        $values = [@$where{@keys}];
    }

    my $dbh = $self->dbh;
    my $columns = $self->_get_select_columns($opt);
    my $cond = join ' AND ', map {$dbh->quote_identifier($_) . ' = ?'} @keys;
    my $sql = sprintf('SELECT %s FROM %s WHERE %s %s',
               join(',', map { ref $_ ? $$_ : $_ } @{$columns}),
               $table_name,
               $cond,
               $opt->{for_update} ? 'FOR UPDATE' : '',
           );

    my $sth = $self->execute($sql, $values);
    return $sth->fetchrow_hashref($self->{fields_case});
}

1;
__END__

=head1 NAME

Karasu::Plugin::Lookup - lookup single row.

=head1 NAME

    package MyDB;
    use parent qw/Karasu/;
    __PACKAGE__->load_plugin('Lookup');

    package main;
    my $db = MyDB->new(...);
    $db->lookup('user' => +{id => 1}); # => get single row

=head1 DESCRIPTION

This plugin provides fast lookup row .

=head1 METHODS

=over 4

=item $row = $db->lookup($table_name, \%search_condition, [\%attr]);

lookup single row records.

Karasu#single is heavy.

=back
