package Karasu::Plugin::Replace;
use strict;
use warnings;
use utf8;

our @EXPORT = qw/replace/;

sub replace {
    my ($self, $table_name, $args) = @_;

    my ($sql, @binds) = $self->sql_builder->insert( $table_name, $args, { prefix => 'REPLACE INTO' } );
    $self->execute($sql, \@binds, $table_name);

    return $self->_last_insert_id($table_name);
}

1;
__END__

=head1 NAME

    Karasu::Plugin::Replace - add replace for Karasu

=head1 PROVIDED METHODS

=over 4

=item $teng->replace($table_name, \%rows_data);

recoed by replace.

    example:

Your::Model->replace('user',
                     {
            id   => 3,
            name => 'walf443',
        },
    );

=back
