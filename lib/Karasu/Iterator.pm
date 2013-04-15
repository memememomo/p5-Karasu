package Karasu::Iterator;
use strict;
use warnings;
use Carp ();

sub new {
    my ($class, %args) = @_;

    return bless \%args, $class;
}

sub next {
    my $self = shift;

    my $row;
    if ($self->{sth}) {
        $row = $self->{sth}->fetchrow_hashref;
        $self->{select_columns} ||= $self->{sth}->{$self->{karasu}->{fields_case}};
        unless ( $row ) {
            $self->{sth}->finish;
            $self->{sth} = undef;
            return;
        }
    } else {
        return ;
    }

    return $row;
}

sub all {
    my $self = shift;

    my $result = [];

    if ($self->{sth}) {
        $self->{select_columns} ||= $self->{sth}->{$self->{karasu}->{fields_case}};
        $result = $self->{sth}->fetchall_arrayref(+{});
        $self->{sth}->finish;
        $self->{sth} = undef;
    }

    return wantarray ? @$result : $result;
}

1;
