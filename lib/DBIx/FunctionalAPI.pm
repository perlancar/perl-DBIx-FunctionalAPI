package DBIx::FunctionalAPI;

use 5.010001;
use strict;
use warnings;
use experimental 'smartmatch';
use Log::Any '$log';

# VERSION
# DATE

use List::MoreUtils qw(uniq);
use Complete::Util qw(complete_array);

require Exporter;
our @ISA       = qw(Exporter);
our @EXPORT_OK = qw(
                       list_tables
                       list_columns
                       list_rows
               );
# TODO: delete_row, delete_rows
# TODO: delete_table, delete_tables
# TODO: create_row, create_table
# TODO: modify_row, modify_table
# TODO: rename_column, rename_table
# TODO: get_table_schema
# TODO: get_db_schema

our $dbh;
our %SPEC;

my %common_args = (
    dbh => {
        summary => 'Database handle',
        schema  => ['obj*'],
    },
);

my %detail_arg = (
    detail => {
        summary => 'Whether to return detailed records instead of just '.
            'items/strings',
        schema  => 'bool',
    },
);

my %table_arg = (
    table => {
        summary => 'Table name',
        schema  => 'str*',
        req => 1,
        pos => 0,
        completion => sub {
            my %args = @_;
            $log->errorf("TMP:%s", \%args);
            my $word = $args{word} // "";
            my $res = list_tables(dbh=>$args{args}{dbh});
            return [] if $res->[0] != 200;

            my $tables = $res->[2];

            # dequote; this is currently ad-hoc for ansi & mysql
            for (@$tables) {
                s/[`"]+//g;
            }
            # provide non-qualified table names for convenience
            my @nonq;
            for my $t (@$tables) {
                $t =~ s/.+\.//;
                push @nonq, $t unless $t ~~ @nonq;
            }
            push @$tables, @nonq;

            $tables = [uniq @$tables];

            complete_array(word=>$word, array=>$tables);
        },
    },
);

$SPEC{list_tables} = {
    v => 1.1,
    args => {
        %common_args,
        # TODO: detail
    },
};
sub list_tables {
    my %args = @_;

    my $dbh = $args{dbh} // $dbh;

    [200, "OK", [$dbh->tables(undef, undef)]];
}

$SPEC{list_columns} = {
    v => 1.1,
    args => {
        %common_args,
        %detail_arg,
        %table_arg,
    },
};
sub list_columns {
    my %args = @_;

    my $dbh = $args{dbh} // $dbh;
    my $table  = $args{table};
    my $detail = $args{detail};

    my @res;
    my $sth = $dbh->column_info(undef, undef, $table, undef);
    while (my $row = $sth->fetchrow_hashref) {
        if ($detail) {
            push @res, {
                name => $row->{COLUMN_NAME},
                type => $row->{TYPE_NAME},
                pos  => $row->{ORDINAL_POSITION},
            };
        } else {
            push @res, $row->{COLUMN_NAME};
        }
    }

    [200, "OK", \@res];
}

$SPEC{list_rows} = {
    v => 1.1,
    args => {
        %common_args,
        %detail_arg,
        %table_arg,
        # TODO: criteria
        # TODO: paging options
        # TODO: ordering options
        # TODO: filtering options
    },
};
sub list_rows {
    my %args = @_;

    my $dbh = $args{dbh} // $dbh;
    my $table  = $args{table};
    my $detail = $args{detail};

    my @rows;
    # can't use placeholder here for table name
    my $sth = $dbh->prepare("SELECT * FROM ".$dbh->quote_identifier($table));
    $sth->execute();
    while (my $row = $sth->fetchrow_hashref) {
        if ($detail) {
            push @rows, $row;
        } else {
            push @rows, $row; # $row->{}; # TODO: check pk
        }
    }

    [200, "OK", \@rows];
}

1;
#ABSTRACT: Some functions to expose your database as an API

=for Pod::Coverage ^()$

=head1 SYNOPSIS


=head1 DESCRIPTION

B<NOTE: EARLY RELEASE AND MINIMAL FUNCTIONALITIES>

This module provides a set of functions to get information and modify your
L<DBI> database. The functions are suitable in RPC-style or stateless
client/server (like HTTP) API.

Every function accepts C<dbh> argument, but for convenience database handle can
also be set via the C<$dbh> package variable.


=head1 TODO

Option to select specific catalog and schema (table namespace).


=head1 SEE ALSO

=cut
