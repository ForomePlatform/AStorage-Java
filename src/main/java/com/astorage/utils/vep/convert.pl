#!/usr/bin/perl
use strict;
use warnings;
use Storable;
use JSON;
use IO::Uncompress::Gunzip qw(gunzip $GunzipError);
use Scalar::Util qw(blessed reftype refaddr);
use List::Util qw(min);

# Usage: perl convert_storable_to_json.pl input.storable output.json
my ($storable_file, $json_file) = @ARGV;

if (!defined $storable_file || !defined $json_file) {
    die "Usage: $0 input.storable output.json\n";
}

sub print_item_info {
    my $item = $_[0];

    if (blessed($item)) {
        print "Encountered a blessed object of type " . ref($item) . "\n";
    }

    if (reftype($item) eq 'HASH') {
        print "Hash properties:\n";
        foreach my $key (keys %$item) {
            print "  $key => " . (defined $item->{$key} ? $item->{$key} : 'undef') . "\n";
        }
    }
    elsif (reftype($item) eq 'ARRAY') {
        print "Array elements:\n";
        for my $i (0 .. $#$item) {
            print "  [$i] => " . (defined $item->[$i] ? $item->[$i] : 'undef') . "\n";
        }
    }
}

sub deobjectify {
    my ($item, $seen, $nextId) = @_;

    my $ref_addr = refaddr($item);
    if (defined $ref_addr && exists $seen->{$ref_addr}) {
        return { '$ref' => "" . $seen->{$ref_addr} };
    }

    if (blessed($item)) {
        if (reftype($item) eq 'HASH') {
            $item = { %$item };
            $seen->{$ref_addr} = $$nextId++ if defined $ref_addr;
            $item->{'$id'} = "" . $seen->{$ref_addr};
        }
        elsif (reftype($item) eq 'ARRAY') {
            $item = [ @$item ];
        }
    }

    if (ref($item) eq 'HASH') {
        foreach my $key (keys %$item) {
            $item->{$key} = deobjectify($item->{$key}, $seen, $nextId);
        }
    }
    elsif (ref($item) eq 'ARRAY') {
        for my $i (0 .. $#$item) {
            $item->[$i] = deobjectify($item->[$i], $seen, $nextId);
        }
    }

    return $item;
}

# Read and deserialize the Storable file
my $data;
open my $fh, '<:raw', $storable_file or die "Could not open '$storable_file': $!";

read($fh, my $magic_byte, 1);
seek($fh, 0, 0);

if ($magic_byte eq "\x1b") {
    my $uncompressed_data;
    gunzip $fh => \$uncompressed_data or die "Gunzip failed: $GunzipError";
    $data = Storable::thaw($uncompressed_data);
}
else {
    $data = Storable::fd_retrieve($fh);
}

close $fh;

my $array = $data->{"1"};
my $result = [];
my $seen = {};
my $nextId = 0;

for my $i (0 .. $#$array) {
    $nextId = 0;
    $result->[$i] = deobjectify($array->[$i], $seen, \$nextId);
}

my $json = JSON->new->utf8;

# Write the JSON data to the output file
open my $out, '>', $json_file or die "Could not open '$json_file': $!";

# Iterate through each object and print it as JSONL
for my $i (0 .. $#$result) {
    my $json_line = $json->encode($result->[$i]);
    print $out $json_line; # Write to the file
    if ($i ne $#$result) {
        print $out "\n";
    }
}

close $out;

print "Successfully converted '$storable_file' to '$json_file'\n";